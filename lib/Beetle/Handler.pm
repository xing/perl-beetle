package Beetle::Handler;

use Moose;
extends qw(Beetle::Base);
use Data::Dumper;
use Scalar::Util;
use Class::MOP;

has 'message' => (
    is  => 'rw',
    isa => 'Any',
);

has 'processor' => (
    is        => 'ro',
    isa       => 'CodeRef',
    predicate => 'has_processor',
);

has 'errback' => (
    is        => 'ro',
    isa       => 'CodeRef',
    predicate => 'has_errback',
);

has 'failback' => (
    is        => 'ro',
    isa       => 'CodeRef',
    predicate => 'has_failback',
);

# def self.create(block_or_handler, opts={}) #:nodoc:
#   if block_or_handler.is_a? Handler
#     block_or_handler
#   elsif block_or_handler.is_a?(Class) && block_or_handler.ancestors.include?(Handler)
#     block_or_handler.new
#   else
#     new(block_or_handler, opts)
#   end
# end
# TODO: <plu> -maybe- adapt the ruby interface, not sure yet
sub create {
    my ( $package, $thing, $args ) = @_;

    $args ||= {};

    if ( defined $thing && ref $thing eq 'CODE' ) {
        return $package->new( processor => $thing, %$args );
    }

    elsif ( defined $thing && Scalar::Util::blessed $thing && grep $_ eq __PACKAGE__, $thing->meta->superclasses ) {
        return $thing;
    }

    elsif ( defined $thing && grep $_ eq __PACKAGE__, $thing->meta->linearized_isa ) {
        return $thing->new(%$args);
    }
}

# # called when a message should be processed. if the message was caused by an RPC, the
# # return value will be sent back to the caller. calls the initialized processor proc
# # if a processor proc was specified when creating the Handler instance. calls method
# # process if no proc was given. make sure to call super if you override this method in
# # a subclass.
# def call(message)
#   @message = message
#   if @processor
#     @processor.call(message)
#   else
#     process
#   end
# end
sub call {
    my ( $self, $message ) = @_;
    $self->message($message);
    if ( $self->has_processor ) {
        return $self->processor->($message);
    }
    else {
        return $self->process;
    }
}

# # called for message processing if no processor was specfied when the handler instance
# # was created
# def process
#   logger.info "Beetle: received message #{message.inspect}"
# end
sub process {
    my ($self) = @_;
    $self->log->info( sprintf 'Beetle: received message %s', Dumper( $self->message ) );
}

# # should not be overriden in subclasses
# def process_exception(exception) #:nodoc:
#   if @error_callback
#     @error_callback.call(message, exception)
#   else
#     error(exception)
#   end
# rescue Exception
#   Beetle::reraise_expectation_errors!
# end
sub process_exception {
    my ( $self, $exception ) = @_;
    if ( $self->has_errback ) {
        return eval { $self->errback->( $self->message, $exception ) };
    }
    else {
        return eval { $self->error($exception) };
    }
}

# # should not be overriden in subclasses
# def process_failure(result) #:nodoc:
#   if @failure_callback
#     @failure_callback.call(message, result)
#   else
#     failure(result)
#   end
# rescue Exception
#   Beetle::reraise_expectation_errors!
# end
sub process_failure {
    my ( $self, $result ) = @_;
    if ( $self->has_failback ) {
        return eval { $self->failback->( $self->message, $result ) };
    }
    else {
        return eval { $self->failure($result) };
    }
}

# # called when handler execution raised an exception and no error callback was
# # specified when the handler instance was created
# def error(exception)
#   logger.error "Beetle: handler execution raised an exception: #{exception}"
# end
sub error {
    my ( $self, $exception ) = @_;
    $self->log->error( sprintf 'Beetle: handler execution raised an exception: %s', $exception );
}

# # called when message processing has finally failed (i.e., the number of allowed
# # handler execution attempts or the number of allowed exceptions has been reached) and
# # no failure callback was specified when this handler instance was created.
# def failure(result)
#   logger.error "Beetle: handler has finally failed"
# end
sub failure {
    my ( $self, $result ) = @_;
    $self->log->error('Beetle: handler has finally failed');
}

1;
