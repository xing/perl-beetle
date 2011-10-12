package    # hide from PAUSE
  Test::Beetle::Handler::Timeout;

use Moose;
extends qw(Beetle::Handler);

has 'process_duration' => (
    default => 0,
    is      => 'rw',
    isa     => 'Int',
);

has 'on_failure' => (
    is        => 'rw',
    isa       => 'CodeRef',
    predicate => 'has_on_failure',
);

has 'on_error' => (
    is        => 'rw',
    isa       => 'CodeRef',
    predicate => 'has_on_error',
);

sub process {
    my ($self) = @_;
    sleep $self->process_duration();
}

sub error {
    my ( $self, $exception ) = @_;
    $self->log->info("execution failed: $exception");
    $self->on_error->($exception) if $self->has_on_error;
}

sub failure {
    my ($self) = @_;
    $self->on_failure->() if $self->has_on_failure;
}

1;
