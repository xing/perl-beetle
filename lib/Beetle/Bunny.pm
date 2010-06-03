package Beetle::Bunny;

use Moose;
use AnyEvent;
use Net::RabbitFoot;

has 'host' => (
    is       => 'rw',
    isa      => 'Str',
    required => 1,
);

has 'port' => (
    is       => 'rw',
    isa      => 'Str',
    required => 1,
);

has 'user' => (
    is       => 'rw',
    isa      => 'Str',
    required => 1,
);

has 'pass' => (
    is       => 'rw',
    isa      => 'Str',
    required => 1,
);

has 'vhost' => (
    is       => 'rw',
    isa      => 'Str',
    required => 1,
);

has 'verbose' => (
    default => 0,
    is      => 'rw',
    isa     => 'Bool',
);

has 'anyevent_condvar' => (
    is  => 'rw',
    isa => 'Any',
);

has '_mq' => (
    isa        => 'Any',
    lazy_build => 1,
    handles    => { _open_channel => 'open_channel', },
);

has '_channel' => (
    default => sub { shift->_open_channel },
    handles => {
        _close            => 'close',
        _bind_queue       => 'bind_queue',
        _consume          => 'consume',
        _declare_exchange => 'declare_exchange',
        _declare_queue    => 'declare_queue',
        _publish          => 'publish',
    },
    isa  => 'Any',
    lazy => 1,
);

sub exchange_declare {
    my ( $self, $exchange, $options ) = @_;
    $options ||= {};
    $self->_declare_exchange(
        exchange => $exchange,
        %$options,
    );
}

sub listen {
    my ($self) = @_;
    my $c = AnyEvent->condvar;
    $self->anyevent_condvar($c);

    # Run the event loop forever
    $c->recv;
}

sub publish {
    my ( $self, $exchange_name, $message_name, $data, $header ) = @_;
    $header ||= {};
    my %data = (
        body        => $data,
        exchange    => $exchange_name,
        routing_key => $message_name,
        header      => $header,
    );
    $self->_publish(%data);
}

sub queue_declare {
    my ( $self, $queue, $options ) = @_;
    $options ||= {};
    $self->_declare_queue(
        queue => $queue,
        %$options,
    );
}

sub queue_bind {
    my ( $self, $queue, $exchange, $routing_key ) = @_;
    $self->_bind_queue(
        exchange    => $exchange,
        queue       => $queue,
        routing_key => $routing_key,
    );
}

sub stop {
    my ($self) = @_;
    $self->anyevent_condvar->send;
    $self->_close;
}

sub subscribe {
    my ( $self, $queue, $callback ) = @_;
    $self->_consume(
        on_consume => $callback,
        queue      => $queue
    );
}

sub _build__mq {
    my ($self) = @_;
    my $rf = Net::RabbitFoot->new( verbose => $self->verbose );
    $rf->connect(
        host  => $self->host,
        port  => $self->port,
        user  => $self->user,
        pass  => $self->pass,
        vhost => $self->vhost,
    );
    return $rf;
}

1;
