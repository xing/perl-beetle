use strict;
use warnings;
use Test::Exception;
use Test::More;

use FindBin qw( $Bin );
use lib ( "$Bin/lib", "$Bin/../lib" );
use TestLib;

BEGIN {
    use_ok('Beetle::Subscriber');
    use_ok('Beetle::Client');
}

done_testing;
