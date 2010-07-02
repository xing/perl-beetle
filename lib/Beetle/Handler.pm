package Beetle::Handler;

use Moose;
use namespace::clean -except => 'meta';
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

sub create {
    my ( $package, $thing, $args ) = @_;

    $args ||= {};

    my @isa = eval { $thing->meta->linearized_isa };

    if ( defined $thing && ref $thing eq 'CODE' ) {
        return $package->new( processor => $thing, %$args );
    }

    elsif ( defined $thing && Scalar::Util::blessed $thing && grep $_ eq $package, @isa ) {
        return $thing;
    }

    elsif ( defined $thing && grep $_ eq $package, @isa ) {
        return $thing->new(%$args);
    }

    else {
        die "Invalid handler";
    }
}

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

sub process {
    my ($self) = @_;
    $self->log->info( sprintf 'Beetle: received message %s', Dumper( $self->message ) );
}

sub process_exception {
    my ( $self, $exception ) = @_;
    if ( $self->has_errback ) {
        return eval { $self->errback->( $self->message, $exception ) };
    }
    else {
        return eval { $self->error($exception) };
    }
}

sub process_failure {
    my ( $self, $result ) = @_;
    if ( $self->has_failback ) {
        return eval { $self->failback->( $self->message, $result ) };
    }
    else {
        return eval { $self->failure($result) };
    }
}

sub error {
    my ( $self, $exception ) = @_;
    $self->log->error( sprintf 'Beetle: handler execution raised an exception: %s', $exception );
}

sub failure {
    my ( $self, $result ) = @_;
    $self->log->error('Beetle: handler has finally failed');
}

__PACKAGE__->meta->make_immutable;

1;
