package Beetle::Base::PubSub;

use Moose;
use namespace::clean -except => 'meta';
extends qw(Beetle::Base);
use Class::MOP;

# Base class for publisher/subscriber

my @QUEUE_CREATION_KEYS = qw(passive durable exclusive auto_delete no_wait);
my @QUEUE_BINDING_KEYS  = qw(key no_wait);

has 'client' => (
    is  => 'ro',
    isa => 'Any',
);

has '_exchanges' => (
    default => sub {
        { shift->server => {} }
    },
    handles => {
        get_exchange => 'get',
        has_exchange => 'exists',
        set_exchange => 'set',
    },
    is     => 'ro',
    isa    => 'HashRef',
    lazy   => 1,
    traits => [qw(Hash)],
);

has 'options' => (
    is  => 'ro',
    isa => 'Any',
);

has '_queues' => (
    default => sub {
        { shift->server => {} }
    },
    handles => {
        get_queue => 'get',
        set_queue => 'set',
    },
    is     => 'ro',
    isa    => 'HashRef',
    lazy   => 1,
    traits => [qw(Hash)],
);

has 'server' => (
    is     => 'ro',
    isa    => 'Any',
    writer => 'set_current_server',
);

# TODO: <plu> maybe a hashref would be more handy
has 'servers' => (
    default => sub { [] },
    handles => {
        add_server    => 'push',
        all_servers   => 'elements',
        get_server    => 'get',
        count_servers => 'count',
    },
    is     => 'ro',
    isa    => 'ArrayRef',
    traits => [qw(Array)],
);

has 'bunnies' => (
    default => sub { {} },
    handles => {
        get_bunny => 'get',
        has_bunny => 'exists',
        set_bunny => 'set',
    },
    is     => 'ro',
    isa    => 'HashRef',
    traits => [qw(Hash)],
);

sub BUILD {
    my ($self) = @_;
    my $servers = $self->client->servers;
    $self->{servers} = $servers;
    $self->{server}  = $servers->[ int rand scalar @$servers ];
}

sub error {
    my ( $self, $message ) = @_;
    $self->log->error($message);
    die $message;
}

sub current_host {
    my ($self) = @_;
    return ( split /:/, $self->server )[0];
}

sub current_port {
    my ($self) = @_;
    my ( $host, $port ) = split /:/, $self->server;
    $port ||= 5672;
    return $port;
}

sub each_server {
    my ( $self, $code ) = @_;

    foreach my $server ( $self->all_servers ) {
        $self->set_current_server($server);
        $code->($self);
    }
}

sub exchanges {
    my ($self) = @_;
    my $exchanges = $self->get_exchange( $self->server );
    return $exchanges || {};
}

sub exchange {
    my ( $self, $name ) = @_;

    my $exchanges = $self->exchanges;

    unless ( defined $exchanges->{$name} ) {
        $exchanges->{$name} = $self->create_exchange( $name => $self->client->get_exchange($name) );
        $self->set_exchange( $self->server => $exchanges );
        return 0;
    }

    return 1;
}

sub queues {
    my ($self) = @_;
    my $queue = $self->get_queue( $self->server );
    $queue ||= {};
    return $queue;
}

sub queue {
    my ( $self, $name ) = @_;

    my $options = $self->client->get_queue($name);
    $self->error("You are trying to bind a queue ${name} which is not configured!") unless $options;

    $self->logger->debug( sprintf 'Beetle: binding queue %s with internal name %s on server %s',
        $name, $options->{amqp_name}, $self->server );

    my $queue_name = $options->{amqp_name};
    my $the_queue;

    my $creation_options = {};
    foreach my $key (@QUEUE_CREATION_KEYS) {
        $creation_options->{$key} = $options->{$key} if exists $options->{$key};
    }

    my $bindings = $self->client->get_binding($name);
    foreach my $row (@$bindings) {
        my $binding_options = {%$row};
        my $exchange_name   = $binding_options->{exchange};
        foreach my $key ( keys %$binding_options ) {
            delete $binding_options->{$key} unless grep $_ eq $key, @QUEUE_BINDING_KEYS;
        }
        $the_queue = $self->bind_queue( $queue_name, $creation_options, $exchange_name, $binding_options );
    }

    $self->set_queue( $self->server => { $name => 1 } );

    return $name;
}

sub bunny {
    my ($self) = @_;
    my $has_bunny = $self->has_bunny( $self->server );
    $self->set_bunny( $self->server => $self->new_bunny ) unless $has_bunny;
    return $self->get_bunny( $self->server );
}

sub new_bunny {
    my ($self) = @_;
    my $class = $self->config->bunny_class;
    Class::MOP::load_class($class);
    return $class->new(
        config => $self->config,
        host   => $self->current_host,
        port   => $self->current_port,
    );
}

sub create_exchange {
    my ( $self, $name, $options ) = @_;
    my %rmq_options = %{ $options || {} };
    delete $rmq_options{queues};
    $self->bunny->exchange_declare( $name => \%rmq_options );
    return 1;
}

__PACKAGE__->meta->make_immutable;

1;
