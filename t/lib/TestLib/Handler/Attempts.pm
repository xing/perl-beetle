package    # hide from PAUSE
  TestLib::Handler::Attempts;

use Moose;
extends qw(Beetle::Handler);

has 'exceptions' => (
    default => 0,
    is      => 'rw',
    isa     => 'Int',
);

has 'on_failure' => (
    is        => 'rw',
    isa       => 'CodeRef',
    predicate => 'has_on_failure',
);

sub process {
    my ($self) = @_;
    $self->exceptions( $self->exceptions + 1 );
    die sprintf 'failed %d times', $self->exceptions;
}

sub error {
    my ( $self, $exception ) = @_;
    $self->log->info("execution failed: $exception");
}

sub failure {
    my ($self) = @_;
    $self->on_failure->() if $self->has_on_failure;
}

1;
