package    # hide from PAUSE
  Test::Beetle::Bunny;

use Moose;

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

sub ack {

    # my ( $self, $options ) = @_;
}

sub exchange_declare {

    # my ( $self, $exchange, $options ) = @_;
}

sub get {

    # my ( $self, $queue, $options ) = @_;
}

sub listen {

    # my ($self) = @_;
}

sub publish {

    # my ( $self, $exchange_name, $message_name, $data, $header ) = @_;
}

sub purge {

    # my ( $self, $queue, $options ) = @_;
}

sub queue_declare {

    # my ( $self, $queue, $options ) = @_;
}

sub queue_bind {

    # my ( $self, $queue, $exchange, $routing_key ) = @_;
}

sub recover {

    # my ( $self, $options ) = @_;
}

sub stop {

    # my ($self) = @_;
}

sub subscribe {

    # my ( $self, $queue, $callback ) = @_;
}

1;
