package Beetle::Message;

use Moose;
use Data::UUID;

my $DEFAULT_ATTEMPTS_LIMIT   = 1;
my $DEFAULT_DELAY            = 10;
my $DEFAULT_EXCEPTIONS_LIMIT = 0;
my $DEFAULT_TIMEOUT          = 300;
my $FLAG_REDUNDANT           = 1;
my $FORMAT_VERSION           = 1;
my $UUID                     = Data::UUID->new();

has 'server' => (
    documentation => 'server from which the message was received',
    is            => 'rw',
    isa           => 'Any',
);

has 'queue' => (
    documentation => 'name of the queue on which the message was received',
    is            => 'rw',
    isa           => 'Any',
);

has 'header' => (
    documentation => 'the AMQP header received with the message',
    is            => 'rw',
    isa           => 'Any',
);

has 'uuid' => (
    documentation => 'the uuid of the message',
    is            => 'rw',
    isa           => 'Any',
);

has 'data' => (
    documentation => 'message payload',
    is            => 'rw',
    isa           => 'Any',
);

has 'format_version' => (
    documentation => 'the message format version of the message',
    is            => 'rw',
    isa           => 'Any',
);

has 'flags' => (
    documentation => 'flags sent with the message',
    is            => 'rw',
    isa           => 'Any',
);

has 'expires_at' => (
    documentation => 'unix timestamp after which the message should be considered stale',
    is            => 'rw',
    isa           => 'Any',
);

has 'timeout' => (
    default       => $DEFAULT_TIMEOUT,
    documentation => 'how many seconds the handler is allowed to execute',
    is            => 'rw',
    isa           => 'Int',
);

has 'delay' => (
    default       => $DEFAULT_DELAY,
    documentation => 'how long to wait before retrying the message handler',
    is            => 'rw',
    isa           => 'Int',
);

has 'attempts_limit' => (
    default       => $DEFAULT_ATTEMPTS_LIMIT,
    documentation => 'how many times we should try to run the handler',
    is            => 'rw',
    isa           => 'Int',
);

has 'exceptions_limit' => (
    default       => $DEFAULT_EXCEPTIONS_LIMIT,
    documentation => 'how many exceptions we should tolerate before giving up',
    is            => 'rw',
    isa           => 'Int',
);

has 'exception' => (
    documentation => 'exception raised by handler execution',
    is            => 'rw',
    isa           => 'Any',
);

has 'handler_result' => (
    documentation => 'value returned by handler execution',
    is            => 'rw',
    isa           => 'Any',
);

sub BUILD {
    my ($self) = @_;
    $self->{attempts_limit} = $self->exceptions_limit + 1 if $self->attempts_limit <= $self->exceptions_limit;
}

sub publishing_options {
    my ( $package, %args ) = @_;

    my $flags = 0;
    $flags |= $FLAG_REDUNDANT if $args{':redundant'};

    my $expires_at = time + $DEFAULT_TTL;

    # TODO: <plu> implement this
    # opts = opts.slice(*PUBLISHING_KEYS)
    $args{':message_id'} = generate_uuid();
    $args{':headers'}    = {
        ':format_version' => $FORMAT_VERSION,
        ':flags'          => $flags,
        ':expires_at'     => $expires_at,
    };

    return wantarray ? %args : \%args;
}

sub generate_uuid {
    return $UUID->create_str();
}
