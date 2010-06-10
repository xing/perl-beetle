use strict;
use warnings;
use Test::More;

use FindBin qw( $Bin );
use lib ( "$Bin/../lib", "$Bin/../../lib" );
use TestLib;
use TestLib::Redis;

test_redis(
    sub {
        my $store = shift;

        {
            my ( $id, $sfx, $val ) = qw(message_id_1 suffix value);
            ok( $store->set( $id, $sfx, $val ), 'Set works' );
            is( $store->get( $id, $sfx ), $val, 'Get works' );
        }

        {
            my $result = $store->msetnx( "message_id_2" => { foo => 'bar', bla => 'fasel' } );
            is( $result, 1, 'New keys added using msetnx' );
        }

        {
            my $result = $store->msetnx( "message_id_2" => { foo => 'bar', bla => 'fasel' } );
            is( $result, 0, 'Keys did already exist, using msetnx' );
        }

        {
            my ( $id, $sfx ) = qw(message_id_3 suffix);
            for ( 1 .. 5 ) {
                my $result = $store->incr( $id, $sfx );
                is( $result, $_, 'Return value of incr call is correct' );
                is( $store->get( $id, $sfx ), $_, 'Verify the value in redis is correct after incr' );
            }
        }

    }
);

done_testing;
