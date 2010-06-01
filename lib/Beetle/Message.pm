package Beetle::Message;

# TODO: <plu> fix 'isa' of all attributes
# TODO: <plu> not sure if we got this :colon implementation right since
#             this seems to be a Ruby singleton string

use Moose;
use Data::UUID;
use Devel::StackTrace;
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
my @PUBLISHING_KEYS = qw(key mandatory immediate persistent reply_to);

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

has 'store' => (
    is  => 'rw',
    isa => 'Beetle::DeduplicationStore',
);

around 'BUILDARGS' => sub {
    my $orig  = shift;
    my $class = shift;

    my %args = @_;

    $args{attempts_limit}   = delete $args{attempts}   if defined $args{attempts};
    $args{exceptions_limit} = delete $args{exceptions} if defined $args{exceptions};

    return $class->$orig(%args);
};

sub BUILD {
    my ($self) = @_;

    $self->{attempts_limit} = $self->exceptions_limit + 1 if $self->attempts_limit <= $self->exceptions_limit;
    $self->decode;
}

# ack the message for rabbit. deletes all keys associated with this message in the
# deduplication store if we are sure this is the last message with the given msg_id.
# def ack!
#   #:doc:
#   logger.debug "Beetle: ack! for message #{msg_id}"
#   header.ack
#   return if simple? # simple messages don't use the deduplication store
#   if !redundant? || @store.incr(msg_id, :ack_count) == 2
#     @store.del_keys(msg_id)
#   end
# end
sub ack {
    my ($self) = @_;
    $self->log->debug( sprintf 'Beetle: ack! for message %s', $self->msg_id );

    # TODO: <plu> implement the ack here! No clue how/why
    # $self->header->ack;
    return if $self->simple;    # simple messages don't use the deduplication store
    if ( !$self->redundant || $self->store->incr( $self->msg_id => 'ack_count' ) == 2 ) {
        $self->store->del_keys( $self->msg_id );
    }
    return;
}

# aquire execution mutex before we run the handler (and delete it if we can't aquire it).
# def aquire_mutex!
#   if mutex = @store.setnx(msg_id, :mutex, now)
#     logger.debug "Beetle: aquired mutex: #{msg_id}"
#   else
#     delete_mutex!
#   end
#   mutex
# end
sub aquire_mutex {
    my ($self) = @_;
    my $mutex;
    if ( $mutex = $self->store->setnx( $self->msg_id => mutex => $self->now ) ) {
        $self->log->debug( sprintf 'Beetle: aquired mutex: %s', $self->msg_id );
    }
    else {
        $self->delete_mutex;
    }
    return $mutex;
}

# how many times we already tried running the handler
# def attempts
#   @store.get(msg_id, :attempts).to_i
# end
sub attempts {
    my ($self) = @_;
    $self->store->get( $self->msg_id => 'attempts' );
}

# whether we have already tried running the handler as often as specified when the handler was registered
# def attempts_limit_reached?
#   (limit = @store.get(msg_id, :attempts)) && limit.to_i >= attempts_limit
# end
sub attempts_limit_reached {
    my ($self) = @_;
    my $limit = $self->store->get( $self->msg_id => 'attempts' );
    return $limit && $limit >= $self->attempts_limit ? 1 : 0;
}

# mark message handling complete in the deduplication store
# def completed!
#   @store.set(msg_id, :status, "completed")
#   timed_out!
# end
sub completed {
    my ($self) = @_;
    $self->store->set( $self->msg_id => status => 'completed' );
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
sub decode {
    my ($self) = @_;

    my $header = $self->header;
    my $amqp_headers = $header->{headers};

    $self->{uuid}           = $header->{message_id};
    $self->{format_version} = $amqp_headers->{format_version};
    $self->{flags}          = $amqp_headers->{flags};
    $self->{expires_at}     = $amqp_headers->{expires_at};
}

# whether we should wait before running the handler
# def delayed?
#   (t = @store.get(msg_id, :delay)) && t.to_i > now
# end
sub delayed {
    my ($self) = @_;
    my $t = $self->store->get( $self->msg_id => 'delay' );
    return $t && $t > $self->now ? 1 : 0;
}

# delete execution mutex
# def delete_mutex!
#   @store.del(msg_id, :mutex)
#   logger.debug "Beetle: deleted mutex: #{msg_id}"
# end
sub delete_mutex {
    my ($self) = @_;
    $self->store->del( $self->msg_id => 'mutex' );
    $self->log->debug( sprintf 'Beetle: deleted mutex: %s', $self->msg_id );
}

# whether the number of exceptions has exceeded the limit set when the handler was registered
# def exceptions_limit_reached?
#   @store.get(msg_id, :exceptions).to_i > exceptions_limit
# end
sub exceptions_limit_reached {
    my ($self) = @_;
    my $value = $self->store->get( $self->msg_id => 'exceptions' );
    return $value > $self->exceptions_limit ? 1 : 0;
}

# a message has expired if the header expiration timestamp is msaller than the current time
# def expired?
#   @expires_at < now
# end
sub expired {
    my ($self) = @_;
    return $self->expires_at < time ? 1 : 0;
}

# generate uuid for publishing
sub generate_uuid {
    return Data::UUID->new->create_str;
}

# increment number of exception occurences in the deduplication store
# def increment_exception_count!
#   @store.incr(msg_id, :exceptions)
# end
sub increment_exception_count {
    my ($self) = @_;
    $self->store->incr( $self->msg_id => 'exceptions' );
}

# record the fact that we are trying to run the handler
# def increment_execution_attempts!
#   @store.incr(msg_id, :attempts)
# end
sub increment_execution_attempts {
    my ($self) = @_;
    $self->store->incr( $self->msg_id => 'attempts' );
}

# message handling completed?
# def completed?
#   @store.get(msg_id, :status) == "completed"
# end
sub is_completed {
    my ($self) = @_;
    my $value = $self->store->get( $self->msg_id => 'status' );
    return $value && $value eq 'completed' ? 1 : 0;
}

# handler timed out?
# def timed_out?
#   (t = @store.get(msg_id, :timeout)) && t.to_i < now
# end
sub is_timed_out {
    my ($self) = @_;
    my $t = $self->store->get( $self->msg_id => 'timeout' );
    return $t && $t < $self->now ? 1 : 0;
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
    my $successful = $self->store->msetnx( $self->msg_id => { status => 'incomplete', expires => $self->expires_at } );
    if ($successful) {
        return 0;
    }
    $self->log->info( sprintf "Beetle: received duplicate message: %s on queue: %s", $self->msg_id, $self->queue );
    return 1;
}

# # unique message id. used to form various keys in the deduplication store.
# def msg_id
#   @msg_id ||= "msgid:#{queue}:#{uuid}"
# end
sub msg_id {
    my ($self) = @_;
    return sprintf "msgid:%s:%s", $self->queue, $self->uuid;
}

# def now #:nodoc:
#   Time.now.to_i
# end
sub now {
    return time(); # TODO: <plu> Hmmm... timezones'n'shit?!
}

# process this message and do not allow any exception to escape to the caller
# def process(handler)
#   logger.debug "Beetle: processing message #{msg_id}"
#   result = nil
#   begin
#     result = process_internal(handler)
#     handler.process_exception(@exception) if @exception
#     handler.process_failure(result) if result.failure?
#   rescue Exception => e
#     Beetle::reraise_expectation_errors!
#     logger.warn "Beetle: exception '#{e}' during processing of message #{msg_id}"
#     logger.warn "Beetle: backtrace: #{e.backtrace.join("\n")}"
#     result = RC::InternalError
#   end
#   result
# end
# TODO: <plu> make sure I got this right.
sub process {
    my ( $self, $handler ) = @_;
    $self->log->debug( sprintf 'Beetle: processing message %s', $self->msg_id );
    my $result;
    eval { $result = $self->_process_internal($handler); };
    if ($@) {
        $handler->process_exception($@);
        my $trace = Devel::StackTrace->new;
        $self->log->warn( sprintf "Beetle: exception '%s' during processing of message %s", $@, $self->msg_id );
        $self->log->warn( sprintf "Beetle: backtrace: %s", $trace->as_string );
        $result = 'RC::InternalError';
    }
    $handler->process_failure($result) if $result eq 'FAILURE'; # TODO: <plu> this is wrong!!!
    return $result;
}

# build hash with options for the publisher
sub publishing_options {
    my ( $package, %args ) = @_;

    my $flags = 0;
    $flags |= $FLAG_REDUNDANT if $args{redundant};

    $args{ttl} = $DEFAULT_TTL unless defined $args{ttl};

    my $expires_at = now() + $args{ttl};

    foreach my $key (keys %args) {
        delete $args{$key} unless grep $_ eq $key, @PUBLISHING_KEYS;
    }

    $args{message_id} = generate_uuid();
    $args{headers}    = {
        format_version => $FORMAT_VERSION,
        flags          => $flags,
        expires_at     => $expires_at,
    };

    return wantarray ? %args : \%args;
}

# whether the publisher has tried sending this message to two servers
# def redundant?
#   @flags & FLAG_REDUNDANT == FLAG_REDUNDANT
# end
sub redundant {
    my ($self) = @_;
    return $self->flags & $FLAG_REDUNDANT ? 1 : 0;
}

# reset handler timeout in the deduplication store
# def timed_out!
#   @store.set(msg_id, :timeout, 0)
# end
sub reset_timeout {
    my ($self) = @_;
    $self->store->set( $self->msg_id => timeout => 0 );
}

# store delay value in the deduplication store
# def set_delay!
#   @store.set(msg_id, :delay, now + delay)
# end
sub set_delay {
    my ($self) = @_;
    $self->store->set( $self->msg_id => delay => $self->now + $self->delay );
}

# store handler timeout timestamp in the deduplication store
# def set_timeout!
#   @store.set(msg_id, :timeout, now + timeout)
# end
sub set_timeout {
    my ($self) = @_;
    $self->store->set( $self->msg_id => timeout => $self->now + $self->timeout);
}

# whether this is a message we can process without accessing the deduplication store
# def simple?
#   !redundant? && attempts_limit == 1
# end
sub simple {
    my ($self) = @_;
    return !$self->redundant && $self->attempts_limit == 1 ? 1 : 0;
}

# def run_handler!(handler)
#   increment_execution_attempts!
#   case result = run_handler(handler)
#   when RC::OK
#     completed!
#     ack!
#     result
#   else
#     handler_failed!(result)
#   end
# end
sub _execute_handler {
    my ( $self, $handler ) = @_;
    $self->increment_execution_attempts;
    my $result = $self->_run_handler($handler);
    if ($result eq 'RC::OK') {
        $self->completed;
        $self->ack;
        return $result;
    }
    else {
        return $self->_handler_failed($result);
    }
}

# def handler_failed!(result)
#   increment_exception_count!
#   if attempts_limit_reached?
#     ack!
#     logger.debug "Beetle: reached the handler execution attempts limit: #{attempts_limit} on #{msg_id}"
#     RC::AttemptsLimitReached
#   elsif exceptions_limit_reached?
#     ack!
#     logger.debug "Beetle: reached the handler exceptions limit: #{exceptions_limit} on #{msg_id}"
#     RC::ExceptionsLimitReached
#   else
#     delete_mutex!
#     timed_out!
#     set_delay!
#     result
#   end
# end
sub _handler_failed {
    my ( $self, $result ) = @_;

    if ( $self->attempts_limit_reached ) {
        $self->ack;
        $self->log->debug( sprintf 'Beetle: reached the handler execution attempts limit: %d on %s',
            $self->attempts_limit, $self->msg_id );
        return 'RC::AttemptsLimitReached';
    }

    elsif ( $self->exceptions_limit_reached ) {
        $self->ack;
        $self->log->debug( sprintf 'Beetle: reached the handle exceptions limit: %d on %s',
            $self->exceptions_limit, $self->msg_id );
        return 'RC::ExceptionsLimitReached';
    }

    else {
        $self->delete_mutex;
        $self->reset_timeout;
        $self->set_delay;
        return $result;
    }
}

# def process_internal(handler)
#   if expired?
#     logger.warn "Beetle: ignored expired message (#{msg_id})!"
#     ack!
#     RC::Ancient
#   elsif simple?
#     ack!
#     run_handler(handler) == RC::HandlerCrash ? RC::AttemptsLimitReached : RC::OK
#   elsif !key_exists?
#     set_timeout!
#     run_handler!(handler)
#   elsif completed?
#     ack!
#     RC::OK
#   elsif delayed?
#     logger.warn "Beetle: ignored delayed message (#{msg_id})!"
#     RC::Delayed
#   elsif !timed_out?
#     RC::HandlerNotYetTimedOut
#   elsif attempts_limit_reached?
#     ack!
#     logger.warn "Beetle: reached the handler execution attempts limit: #{attempts_limit} on #{msg_id}"
#     RC::AttemptsLimitReached
#   elsif exceptions_limit_reached?
#     ack!
#     logger.warn "Beetle: reached the handler exceptions limit: #{exceptions_limit} on #{msg_id}"
#     RC::ExceptionsLimitReached
#   else
#     set_timeout!
#     if aquire_mutex!
#       run_handler!(handler)
#     else
#       RC::MutexLocked
#     end
#   end
# end
sub _process_internal {
    my ( $self, $handler ) = @_;

    # TODO: <plu> fix return codes
    if ( $self->expired ) {
        $self->log->warn( sprintf 'Beetle: ignored expired message (%s)', $self->msg_id );
        $self->ack;
        return 'RC::Ancient';
    }

    elsif ( $self->simple ) {
        $self->ack;
        my $result = $self->_run_handler($handler);
        return $result eq 'RC::HandlerCrash' ? 'RC::AttemptsLimitReached' : 'RC::OK';
    }

    elsif ( !$self->key_exists ) {
        $self->set_timeout;
        return $self->_execute_handler($handler);
    }

    elsif ( $self->is_completed ) {
        $self->ack;
        return 'RC::OK';
    }

    elsif ( $self->delayed ) {
        $self->log->warn( sprintf 'Beetle: ignored delayed message (%s)!', $self->msg_id );
        return 'RC::Delayed';
    }

    elsif ( $self->is_timed_out ) {
        return 'RC::HandlerNotYetTimedOut';
    }

    elsif ( $self->attempts_limit_reached ) {
        $self->ack;
        $self->log->warn( sprintf 'Beetle: reached the handler execution attempts limit: %d on %s',
            $self->attempts_limit, $self->msg_id );
        return 'RC::AttemptsLimitReached';
    }

    elsif ( $self->exceptions_limit_reached ) {
        $self->ack;
        $self->log->warn( sprintf 'Beetle: reached the handler exceptions attempts limit: %d on %s',
            $self->exceptions_limit, $self->msg_id );
        return 'RC::ExceptionsLimitReached';
    }

    else {
        $self->set_timeout;
        if ( $self->aquire_mutex ) {
            $self->_execute_handler($handler);
        }
        else {
            return 'RC::MutexLocked';
        }
    }
}

# def run_handler(handler)
#   Timeout::timeout(@timeout) { @handler_result = handler.call(self) }
#   RC::OK
# rescue Exception => @exception
#   Beetle::reraise_expectation_errors!
#   logger.debug "Beetle: message handler crashed on #{msg_id}"
#   RC::HandlerCrash
# ensure
#   ActiveRecord::Base.clear_active_connections! if defined?(ActiveRecord)
# end
sub _run_handler {
    my ( $self, $handler ) = @_;

    # TODO: <plu> implement timeout here - not sure if this is a -really- good idea
    my $result = eval { $handler->call($self); };
    return 'RC::OK' unless $@;

    $self->log->error( sprintf 'Beetle: message handler crashed on %s', $self->msg_id );
    $self->log->error($@);
    return 'RC::HandlerCrash';
}

1;
