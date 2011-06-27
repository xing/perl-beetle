package Beetle::Client;

use Moose;
use namespace::clean -except => 'meta';
use Beetle::DeduplicationStore;
use Beetle::Publisher;
use Beetle::Subscriber;
use Sys::Hostname;
use Net::AMQP::Protocol;
use Net::RabbitFoot;
use AnyEvent::RabbitMQ;

extends qw(Beetle::Base);

=head1 NAME

Beetle::Client - Interface to subscriber and publisher

=head1 SYNOPSIS

    use Beetle::Client;

    my $client = Beetle::Client->new;

    $client->register_queue('test');
    $client->purge('test');
    $client->register_message( test => { redundant => 0 } );

    for ( 1 .. 5 ) {
        $client->publish( test => "Hello $_ world!" );
    }

=head1 DESCRIPTION

This class provides the interface through which messaging is configured for both
message producers and consumers. It keeps references to an instance of a
Beetle::Subscriber, a Beetle::Publisher (both of which are instantiated on demand),
and a reference to an instance of Beetle::DeduplicationStore.

Configuration of exchanges, queues, messages, and message handlers is done by calls to
corresponding register_ methods. Note that these methods just build up the
configuration, they don't interact with the AMQP servers.

On the publisher side, publishing a message will ensure that the exchange it will be
sent to, and each of the queues bound to the exchange, will be created on demand. On
the subscriber side, exchanges, queues, bindings and queue subscriptions will be
created when the application calls the listen method. An application can decide to
subscribe to only a subset of the configured queues by passing a list of queue names
to the listen method.

The net effect of this strategy is that producers and consumers can be started in any
order, so that no message is lost if message producers are accidentally started before
the corresponding consumers.

=cut

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
        AnyEvent::RabbitMQ->load_xml_spec();
    }
}

=head1 METHODS

=head2 new

There are two possible params which can be passed to the constructor:

=over 4

=item * configfile: Scalar

=item * config: HashRef

=back

    my $client = Beetle::Client->new( configfile => '/etc/beetle.yml' );

This will use L<MooseX::SimpleConfig> to load the config file and set
valid attributes of L<Beetle::Config>.

You can also set the attributes of L<Beetle::Config> directly using
a HashRef:

    my $client = Beetle::Client->new(
        config => {
            servers  => 'localhost:5673 localhost:5672',
            loglevel => 'INFO',
        }
    );

It's not possible to set both parameters! The config hash superseeds
the configfile parameter.

=head2 listen

After setting everything up for the subscriber side you need to call this
method to listen for messages on the RabbitMQ servers. If you do not pass
any parameter to this method it will listen for -all- message that you have
configured using L</register_message>. Maybe you have configured some
message(s) you only want to use for publishing messages but not for
subscribing. In that case you need to tell this method to listen only to
certain messages.

    my $client = Beetle::Client->new;
    $client->register_queue('test1');
    $client->register_message( test1 => { redundant => 0 } );
    $client->register_message( test2 => { redundant => 0 } );
    $client->register_message( test3 => { redundant => 0 } );
    $client->publish( test2 => "Hello 2." );
    $client->publish( test3 => "Hello 3." );
    $client->register_handler(
        test1 => sub {
            # ...
        }
    );
    $client->listen( [qw(test1)] );

So the first optional parameter is an ArrayRef of message names you want
to listen for. There's a second optional parameter which may be a CodeRef.
This CodeRef is executed before the event loop starts to listen for AMQP
messages:

    $client->listen( [qw(test1)], sub { warn "Starting to listen now..." } );

=cut

sub listen {
    my ( $self, $messages, $block ) = @_;
    $messages ||= [ $self->message_names ];
    foreach my $message (@$messages) {
        die "unknown message ${message}" unless $self->has_message($message);
    }

    $self->subscriber->listen( $messages, $block );
}

=head2 publish

=cut

sub publish {
    my ( $self, $message_name, $data, $options ) = @_;

    die "unknown message ${message_name}" unless $self->has_message($message_name);

    $options ||= $self->get_message($message_name);

    $self->publisher->publish( $message_name, $data, $options );
}

=head2 purge

There is one param which can be passed to this method:

=over 4

=item * $queue_name (mandatory)

=back

This purges the queue on the RabbitMQ servers. Be careful!

=cut

sub purge {
    my ( $self, $queue_name ) = @_;

    die "unknown queue ${queue_name}" unless $self->has_queue($queue_name);

    $self->publisher->purge($queue_name);
}

=head2 register_binding

There are two params which can be passed to this method:

=over 4

=item * $queue_name (mandatory)

=item * $options (optional)

=back

To receive messages you need to register a binding. The first parameter
is the queue name. In the C<< $options >> HashRef you can define which
exchange and which routhing key this queue should be bound to:

    {
        exchange => 'some_exchange',
        key      => 'some_routing_key',
    }

If you do not provide this options HashRef the exchange and the (routing) key
will default to the queue name.

=cut

sub register_binding {
    my ( $self, $queue_name, $options ) = @_;
    $options ||= {};

    my $exchange = $options->{exchange} || $queue_name;
    my $key      = $options->{key}      || $queue_name;

    $self->register_exchange($exchange) unless $self->has_exchange($exchange);
    $self->_add_binding( $queue_name => { exchange => $exchange, key => $key } );

    my $queues = $self->get_exchange($exchange)->{queues};
    $queues ||= [];

    push @$queues, $queue_name unless grep $_ eq $queue_name, @$queues;
    $self->get_exchange($exchange)->{queues} = $queues;
}

=head2 register_exchange

There are two params which can be passed to this method:

=over 4

=item * $exchange_name (mandatory)

=item * $options (optional)

=back

The C<< $exchange_name >> is a string containing the exchange name. The second
parameter C<< $options >> can be a HashRef containing some options for the
exchange. We override following keys in the C<< $options >> HashRef everytime
to those fixed values:

    {
        durable => 1,
        type    => 'topic',
    }

If you register the same exchange name twice, L<Beetle::Client> will throw an
error!

=cut

sub register_exchange {
    my ( $self, $name, $options ) = @_;
    $options ||= {};

    die "exchange ${name} already configured" if $self->has_exchange($name);

    $options->{durable} = 1;
    $options->{type}    = 'topic';

    $self->set_exchange( $name => $options );
}

=head2 register_handler

=cut

sub register_handler {
    my ( $self, $queues, $handler, $handler_args ) = @_;
    $handler_args ||= {};
    $queues = [$queues] unless ref $queues eq 'ARRAY';

    foreach my $queue (@$queues) {
        die "unknown queue: $queue" unless $self->has_queue($queue);
    }

    $self->subscriber->register_handler( $queues, $handler_args, $handler );
}

=head2 register_message

There are two params which can be passed to this method:

=over 4

=item * $message_name (mandatory)

=item * $options (optional)

=back

The C<< $message_name >> is a string containing the message name. The second
parameter C<< $options >> can be a HashRef containing options for this queue:

    {
        exchange => 'some_exchange',
        key      => 'some_routing_key',
    }

If you do not provide this options HashRef the exchange and the (routing) key
will default to the queue name. You need to register all messages you want
to publish later using the L<Beetle::Client/publish> method. So registered
messages are some kind of alias to publishing options. We override the
following keys in the C<< $options >> HashRef:

    {
        persistent => 1,
    }

    my $client = Beetle::Client->new;
    $client->register_message(
        exception => {
            exchange => 'logmessages',
            key      => 'logmessages.exceptions',
        }
    );
    $client->register_message(
        warning => {
            exchange => 'logmessages',
            key      => 'logmessages.warnings',
        }
    );
    $client->publish( warning   => 'the light is off' );
    $client->publish( exception => 'the light bulb is broken' );

If you register the same message name twice, L<Beetle::Client> will throw an
error!

=cut

sub register_message {
    my ( $self, $message_name, $options ) = @_;
    $options ||= {};

    die "message ${message_name} already configured" if $self->has_message($message_name);

    $options->{exchange} ||= $message_name;
    $options->{key}      ||= $message_name;
    $options->{persistent} = 1;

    $self->set_message( $message_name => $options );
}

=head2 register_queue

There are two params which can be passed to this method:

=over 4

=item * $queue_name (mandatory)

=item * $options (optional)

=back

The C<< $queue_name >> is a string containing the queue name. The second
parameter C<< $options >> can be a HashRef containing options for this queue:

    {
        exchange => 'some_exchange',
        key      => 'some_routing_key',
    }

If you do not provide this options HashRef the exchange and the (routing) key
will default to the queue name.

    my $client = Beetle::Client->new;
    $client->register_queue(
        exceptions => {
            exchange => 'logmessages',
            key      => 'logmessages.exceptions',
        }
    );

If you register the same queue name twice, L<Beetle::Client> will throw an
error!

=cut

sub register_queue {
    my ( $self, $name, $options ) = @_;
    $options ||= {};

    die "queue ${name} already configured" if $self->has_queue($name);

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

=head2 stop_listening

This will stop the listener by calling L<Beetle::Subscriber/stop>.

=cut

sub stop_listening {
    my ($self) = @_;
    $self->subscriber->stop;
}

=head2 stop_publishing

This will stop the publisher by calling L<Beetle::Publisher/stop>.

=cut

sub stop_publishing {
    my ($self) = @_;
    $self->publisher->stop;
}

sub _add_binding {
    my ( $self, $queue_name, $item ) = @_;
    $self->set_binding( $queue_name => [] ) unless $self->has_binding($queue_name);
    my $binding = $self->get_binding($queue_name);
    push @$binding, $item;
    $self->set_binding( $queue_name => $binding );
}

__PACKAGE__->meta->make_immutable;

=head1 AUTHOR

See L<Beetle>.

=head1 COPYRIGHT AND LICENSE

See L<Beetle>.

=cut

1;
