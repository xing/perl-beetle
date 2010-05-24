package Beetle::Message;

# TODO: <plu> fix 'isa' of all attributes
# TODO: <plu> not sure if we got this :colon implementation right since
#             this seems to be a Ruby singleton string

use Moose;
use Data::UUID;
extends qw(Beetle::Base);

# current message format version
our $FORMAT_VERSION = 1;
# flag for encoding redundant messages
my $FLAG_REDUNDANT = 1;
# default lifetime of messages
our $DEFAULT_TTL = 86400;
# forcefully abort a running handler after this many seconds.
# can be overriden when registering a handler.
my $DEFAULT_HANDLER_TIMEOUT = 300;
# how many times we should try to run a handler before giving up
my $DEFAULT_HANDLER_EXECUTION_ATTEMPTS = 1;
# how many seconds we should wait before retrying handler execution
my $DEFAULT_HANDLER_EXECUTION_ATTEMPTS_DELAY = 10;
# how many exceptions should be tolerated before giving up
my $DEFAULT_EXCEPTION_LIMIT = 0;
# AMQP options for message publishing
my @PUBLISHING_KEYS = (':key', ':mandatory', ':immediate', ':persistent', ':reply_to');

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
    $self->_decode;
}

sub publishing_options {
    my ( $package, %args ) = @_;

    my $flags = 0;
    $flags |= $FLAG_REDUNDANT if $args{':redundant'};

    $args{':ttl'} ||= $DEFAULT_TTL;

    my $expires_at = now() + $args{':ttl'};

    foreach my $key (keys %args) {
        delete $args{$key} unless grep $_ eq $key, @PUBLISHING_KEYS;
    }

    $args{':message_id'} = generate_uuid();
    $args{':headers'}    = {
        ':format_version' => $FORMAT_VERSION,
        ':flags'          => $flags,
        ':expires_at'     => $expires_at,
    };

    return wantarray ? %args : \%args;
}

sub generate_uuid {
    return Data::UUID->new->create_str;
}

# def redundant?
#   @flags & FLAG_REDUNDANT == FLAG_REDUNDANT
# end
sub redundant {
    my ($self) = @_;
    return $self->flags & $FLAG_REDUNDANT ? 1 : 0;
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
    my ($self) = @_;

    my $amqp_headers = $self->header->properties;
    my $headers      = $amqp_headers->{':headers'};

    $self->{uuid}           = $amqp_headers->{':message_id'};
    $self->{format_version} = $headers->{':format_version'};
    $self->{flags}          = $headers->{':flags'};
    $self->{expires_at}     = $headers->{':expires_at'};
}

# def now #:nodoc:
#   Time.now.to_i
# end
sub now {
    return time(); # TODO: <plu> Hmmm... timezones'n'shit?!
}

# # unique message id. used to form various keys in the deduplication store.
# def msg_id
#   @msg_id ||= "msgid:#{queue}:#{uuid}"
# end
sub msg_id {
    my ($self) = @_;
    return sprintf "msgid:%s:%s", $self->queue, $self->uuid;
}

# # have we already seen this message? if not, set the status to "incomplete" and store
# # the message exipration timestamp in the deduplication store.
# def key_exists?
#   old_message = 0 == @store.msetnx(msg_id, :status =>"incomplete", :expires => @expires_at)
#   if old_message
#     logger.debug "Beetle: received duplicate message: #{msg_id} on queue: #{@queue}"
#   end
#   old_message
# end
sub key_exists {
    my ($self) = @_;
    my $old_message = 0;
    $old_message = $self->store->msetnx( $self->msg_id => { status => 'incomplete', expires => $self->expires_at } );
    if ($old_message) {
        $self->log->info( "Beetle: received duplicate message: %s on queue: %s", $self->msg_id, $self->queue );
    }
    return $old_message;
}

1;
