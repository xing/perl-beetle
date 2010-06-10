package    # hide from PAUSE
  TestLib::Handler::Attempts;

use Moose;
extends qw(Beetle::Handler);

has 'exceptions' => (
    default => 0,
    is      => 'rw',
    isa     => 'Int',
);

has 'client' => (
    is        => 'rw',
    isa       => 'Any',
    predicate => 'has_client',
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
    $self->client->stop_listening if $self->has_client;
}

1;
