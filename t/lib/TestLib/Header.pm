package    # hide from PAUSE
  TestLib::Header;

use Moose;

has 'properties' => (
    is  => 'rw',
    isa => 'HashRef',
);

1;
