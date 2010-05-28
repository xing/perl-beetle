package    # hide from PAUSE
  TestLib::Handler::SubFooBar;

use Moose;
extends qw(TestLib::Handler::FooBar);

sub process {
    die "something";
}

1;
