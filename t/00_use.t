use Test::More tests => 2;

BEGIN {
  use_ok('Beetle');
  use_ok('Beetle::Message');
}

diag( "Testing Beetle $Beetle::VERSION" );
