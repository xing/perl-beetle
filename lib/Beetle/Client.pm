package Beetle::Client;

# This class provides the interface through which messaging is configured for both
# message producers and consumers. It keeps references to an instance of a
# Beetle::Subscriber, a Beetle::Publisher (both of which are instantiated on demand),
# and a reference to an instance of Beetle::DeduplicationStore.
#
# Configuration of exchanges, queues, messages, and message handlers is done by calls to
# corresponding register_ methods. Note that these methods just build up the
# configuration, they don't interact with the AMQP servers.
#
# On the publisher side, publishing a message will ensure that the exchange it will be
# sent to, and each of the queues bound to the exchange, will be created on demand. On
# the subscriber side, exchanges, queues, bindings and queue subscriptions will be
# created when the application calls the listen method. An application can decide to
# subscribe to only a subset of the configured queues by passing a list of queue names
# to the listen method.
#
# The net effect of this strategy is that producers and consumers can be started in any
# order, so that no message is lost if message producers are accidentally started before
# the corresponding consumers.

use Moose;
use namespace::clean -except => 'meta';
use Beetle::DeduplicationStore;
use Beetle::Publisher;
use Beetle::Subscriber;
use Sys::Hostname;
use Net::AMQP::Protocol;
use Net::RabbitFoot;
extends qw(Beetle::Base);

has 'servers' => (
    documentation => 'the AMQP servers available for publishing',
    is            => 'ro',
    isa           => 'ArrayRef',
);

has 'exchanges' => (
    default       => sub { {} },
    documentation => 'an options hash for the configured exchanges',
    handles       => {
        get_exchange => 'get',
        has_exchange => 'exists',
        set_exchange => 'set',
    },
    is     => 'rw',
    isa    => 'HashRef',
    traits => [qw(Hash)],
);

has 'queues' => (
    default       => sub { {} },
    documentation => 'an options hash for the configured queues',
    handles       => {
        all_queues => 'elements',
        get_queue  => 'get',
        has_queue  => 'exists',
        set_queue  => 'set',
    },
    is     => 'rw',
    isa    => 'HashRef',
    traits => [qw(Hash)],
);

has 'bindings' => (
    default       => sub { {} },
    documentation => 'an options hash for the configured queue bindings',
    handles       => {
        get_binding => 'get',
        has_binding => 'exists',
        set_binding => 'set',
    },
    is     => 'rw',
    isa    => 'HashRef',
    traits => [qw(Hash)],
);

has 'messages' => (
    default       => sub { {} },
    documentation => 'an options hash for the configured messages',
    handles       => {
        get_message   => 'get',
        has_message   => 'exists',
        message_names => 'keys',
        set_message   => 'set',
    },
    is     => 'rw',
    isa    => 'HashRef',
    traits => [qw(Hash)],
);

has 'deduplication_store' => (
    documentation => 'the deduplication store to use for this client',
    is            => 'ro',
    isa           => 'Beetle::DeduplicationStore',
);

has 'publisher' => (
    default => sub {
        my ($self) = @_;
        Beetle::Publisher->new( client => $self, config => $self->config );
    },
    is   => 'ro',
    isa  => 'Beetle::Publisher',
    lazy => 1,
);

has 'subscriber' => (
    default => sub {
        my ($self) = @_;
        Beetle::Subscriber->new( client => $self, config => $self->config );
    },
    is   => 'ro',
    isa  => 'Beetle::Subscriber',
    lazy => 1,
);

sub BUILD {
    my ($self) = @_;
    $self->{deduplication_store} = Beetle::DeduplicationStore->new(

        # TODO: <plu> $self->config should be enough, right?!
        config => $self->config,
        hosts  => $self->config->redis_hosts,
        db     => $self->config->redis_db,
    );
    $self->{servers} = [ split /[ ,]/, $self->config->servers ];

    # Init AMQP spec
    # TODO: <plu> is there no fucking valid way to check if this is done already or not?!
    unless ($Net::AMQP::Protocol::VERSION_MAJOR) {
        Net::AMQP::Protocol->load_xml_spec( Net::RabbitFoot::default_amqp_spec() );
    }
}

sub register_exchange {
    my ( $self, $name, $options ) = @_;
    $options ||= {};

    die "exchange ${name} already configured" if $self->has_exchange($name);

    $options->{durable} = 1;
    $options->{type}    = 'topic';

    $self->set_exchange( $name => $options );
}

sub register_queue {
    my ( $self, $name, $options ) = @_;
    $options ||= {};

    die "queue ${name} already configured" if $self->has_queue($name);

    # TODO: <plu> not sure if i got this opts.merge! right here...
    $options->{exchange} ||= $name;
    $options->{key}      ||= $name;
    $options->{durable}     = 1;
    $options->{passive}     = 0;
    $options->{exclusive}   = 0;
    $options->{auto_delete} = 0;
    $options->{amqp_name}   = $name;

    my $exchange = delete $options->{exchange};
    my $key      = delete $options->{key};

    $self->set_queue( $name => $options );
    $self->register_binding( $name => { exchange => $exchange, key => $key } );
}

sub register_binding {
    my ( $self, $queue_name, $options ) = @_;
    $options ||= {};

    my $exchange = $options->{exchange} || $queue_name;
    my $key      = $options->{key}      || $queue_name;

    $self->add_binding( $queue_name => { exchange => $exchange, key => $key } );
    $self->register_exchange($exchange) unless $self->has_exchange($exchange);

    my $queues = $self->get_exchange($exchange)->{queues};
    $queues ||= [];

    push @$queues, $queue_name unless grep $_ eq $queue_name, @$queues;
    $self->get_exchange($exchange)->{queues} = $queues;

    # TODO: <plu> not sure if I got this right.
}

sub add_binding {
    my ( $self, $queue_name, $item ) = @_;
    $self->set_binding( $queue_name => [] ) unless $self->has_binding($queue_name);
    my $binding = $self->get_binding($queue_name);
    push @$binding, $item;
    $self->set_binding( $queue_name => $binding );
}

sub register_message {
    my ( $self, $message_name, $options ) = @_;
    $options ||= {};

    die "message ${message_name} already configured" if $self->has_message($message_name);
    $options->{exchange} ||= $message_name;
    $options->{key}      ||= $message_name;
    $options->{persistent} = 1;

    $self->set_message( $message_name => $options );
}

sub register_handler {
    my ( $self, $queues, $handler, $handler_args ) = @_;
    $handler_args ||= {};
    $queues = [$queues] unless ref $queues eq 'ARRAY';

    foreach my $queue (@$queues) {
        die "unknown queue: $queue" unless $self->has_queue($queue);    # TODO: <plu> add proper exception handling
    }

    $self->subscriber->register_handler( $queues, $handler_args, $handler );
}

sub publish {
    my ( $self, $message_name, $data, $options ) = @_;
    $options ||= {};

    die "unknown message ${message_name}" unless $self->has_message($message_name);

    $self->publisher->publish( $message_name, $data, $options );
}

sub purge {
    my ( $self, $queue_name ) = @_;

    die "unknown queue ${queue_name}" unless $self->has_queue($queue_name);

    $self->publisher->purge($queue_name);
}

sub listen {
    my ( $self, $messages, $block ) = @_;
    $messages ||= [ $self->message_names ];
    foreach my $message (@$messages) {
        die "unknown message ${message}" unless $self->has_message($message);
    }

    $self->subscriber->listen( $messages, $block );
}

sub stop_listening {
    my ($self) = @_;
    $self->subscriber->stop;
}

sub stop_publishing {
    my ($self) = @_;
    $self->publisher->stop;
}

__PACKAGE__->meta->make_immutable;

1;
