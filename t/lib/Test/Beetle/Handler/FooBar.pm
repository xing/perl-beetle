package    # hide from PAUSE
  Test::Beetle::Handler::FooBar;

use Moose;
extends qw(Beetle::Handler);

sub process {
    my ($self) = @_;
    return uc $self->message;
}

1;
