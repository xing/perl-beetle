use Test::More;

use strict;
use warnings;
use FindBin qw( $Bin );
use lib ( "$Bin/lib", "$Bin/../lib" );
use TestLib;
use TestLib::Redis;
use Beetle::Message;

test_redis(
    sub {
        my $store = shift;

        {
            $store->flushdb;
            my $header = TestLib->header_with_params();
            my $m      = Beetle::Message->new(
                body   => 'foo',
                header => $header,
                queue  => "somequeue",
                store  => $store
            );
            my @keys = $store->keys( $m->msg_id );
            foreach my $key (@keys) {
                is( $m->msg_id, $store->msg_id($key), 'should be able to extract msg_id from any key' );
            }
        }

        {
            $store->flushdb;
            no warnings 'redefine';
            *Beetle::Config::gc_threshold = sub { return 0; };
            *Beetle::Config::logger       = sub { '/dev/null' };
            my $header = TestLib->header_with_params( ttl => 0 );
            my $m = Beetle::Message->new(
                body   => 'foo',
                header => $header,
                queue  => "somequeue",
                store  => $store
            );
            is( $m->key_exists, 0, 'Key did not exist yet' );
            is( $m->key_exists, 1, 'Key exists' );

            is( scalar( $store->redis->keys('*') ), 2, 'Keys are really in store (status + expires)' );
            ok( $store->garbage_collect_keys( time + 1 ), 'Garbage collection' );
            is( scalar( $store->redis->keys('*') ), undef, 'Keys have been removed from store' );
        }

        {
            $store->flushdb;
            no warnings 'redefine';
            *Beetle::Config::gc_threshold = sub { return 0; };
            *Beetle::Config::logger       = sub { '/dev/null' };
            my $header = TestLib->header_with_params( ttl => 60 );
            my $m = Beetle::Message->new(
                body   => 'foo',
                header => $header,
                queue  => "somequeue",
                store  => $store
            );
            is( $m->key_exists, 0, 'Key did not exist yet' );
            is( $m->key_exists, 1, 'Key exists' );

            is( scalar( $store->redis->keys('*') ), 2, 'Keys are really in store (status + expires)' );
            ok( $store->garbage_collect_keys( time + 1 ), 'Garbage collection' );
            is( scalar( $store->redis->keys('*') ), 2, 'Keys are still store' );
            ok( $store->garbage_collect_keys( time + 61 ), 'Garbage collection' );
            is( scalar( $store->redis->keys('*') ), undef, 'Keys have been removed from store' );
        }

    }
);

done_testing;
