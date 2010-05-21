use Test::More;

use strict;
use warnings;
use FindBin qw( $Bin );
use lib ( "$Bin/lib", "$Bin/../lib" );
use TestLib::Redis;

test_redis(
    sub {
        my $store = shift;
        my ( $id, $sfx, $val ) = qw(message_id suffix value);
        ok( $store->set( $id, $sfx, $val ), 'Set works' );
        is( $store->get( $id, $sfx ), $val, 'Get works' );
    }
);

done_testing;
