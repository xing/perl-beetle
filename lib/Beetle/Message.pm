package Beetle::Message;

use Moose;
use Data::UUID;

# current message format version
our $FORMAT_VERSION = 1;
# flag for encoding redundant messages
my $FLAG_REDUNDANT = 1;
# default lifetime of messages
my $DEFAULT_TTL = 86400;
# forcefully abort a running handler after this many seconds.
# can be overriden when registering a handler.
my $DEFAULT_HANDLER_TIMEOUT = 300;
# how many times we should try to run a handler before giving up
my $DEFAULT_HANDLER_EXECUTION_ATTEMPTS = 1;
# how many seconds we should wait before retrying handler execution
my $DEFAULT_HANDLER_EXECUTION_ATTEMPTS_DELAY = 10;
# how many exceptions should be tolerated before giving up
my $DEFAULT_EXCEPTION_LIMIT = 0;

has 'server' => (
    documentation => 'server from which the message was received',
    is            => 'rw',
    isa           => 'Any',
);

has 'queue' => (
    documentation => 'name of the queue on which the message was received',
    is            => 'rw',
    isa           => 'Any',
    required      => 1,
);

has 'header' => (
    documentation => 'the AMQP header received with the message',
    is            => 'rw',
    isa           => 'Any',
    required      => 1,
);

has 'body'  => (
    documentation => '',
    is            => 'rw',
    isa           => 'Any',
    required      => 1,
);

has 'uuid' => (
    documentation => 'the uuid of the message',
    is            => 'ro',
    isa           => 'Data::UUID',
    builder       => '_build_data_uuid',
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
    default       => $DEFAULT_HANDLER_TIMEOUT,
    documentation => 'how many seconds the handler is allowed to execute',
    is            => 'rw',
    isa           => 'Int',
);

has 'delay' => (
    default       => $DEFAULT_HANDLER_EXECUTION_ATTEMPTS_DELAY,
    documentation => 'how long to wait before retrying the message handler',
    is            => 'rw',
    isa           => 'Int',
);

has 'attempts_limit' => (
    default       => $DEFAULT_HANDLER_EXECUTION_ATTEMPTS,
    documentation => 'how many times we should try to run the handler',
    is            => 'rw',
    isa           => 'Int',
);

has 'exceptions_limit' => (
    default       => $DEFAULT_EXCEPTION_LIMIT,
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
    _decode()
}

sub publishing_options {
    my ( $package, %args ) = @_;

    my $flags = 0;
    $flags |= $FLAG_REDUNDANT if $args{':redundant'};

    my $expires_at = time + $DEFAULT_TTL;

    # TODO: <plu> implement this
    # opts = opts.slice(*PUBLISHING_KEYS)
    $args{':message_id'} = generate_uuid()->create_str();
    $args{':headers'}    = {
        ':format_version' => $FORMAT_VERSION,
        ':flags'          => $flags,
        ':expires_at'     => $expires_at,
    };

    return wantarray ? %args : \%args;
}

sub generate_uuid {
    my ($self) = @_;
    return $self->uuid;
}

# def decode #:nodoc:
#   amqp_headers = header.properties
#   @uuid = amqp_headers[:message_id]
#   headers = amqp_headers[:headers]
#   @format_version = headers[:format_version].to_i
#   @flags = headers[:flags].to_i
#   @expires_at = headers[:expires_at].to_i
# end
# extracts various values form the AMQP header properties
sub _decode {

}

# def self.publishing_options(opts = {}) #:nodoc:
#   flags = 0
#   flags |= FLAG_REDUNDANT if opts[:redundant]
#   expires_at = now + (opts[:ttl] || DEFAULT_TTL)
#   opts = opts.slice(*PUBLISHING_KEYS)
#   opts[:message_id] = generate_uuid.to_s
#   opts[:headers] = {
#     :format_version => FORMAT_VERSION.to_s,
#     :flags => flags.to_s,
#     :expires_at => expires_at.to_s
#   }
#   opts
# end
# build hash with options for the publisher
sub _publishing_options {

}

sub _build_data_uuid {
    return Data::UUID->new();
}

1;
