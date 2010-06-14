package    # hide from PAUSE
  Test::Beetle::Handler::SubFooBar;

use Moose;
extends qw(Test::Beetle::Handler::FooBar);

sub process {
    die "something";
}

1;
