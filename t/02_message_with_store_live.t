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

        # test "should be able to extract msg_id from any key" do
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

        # test "should be able to garbage collect expired keys" do
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

        # test "should not garbage collect not yet expired keys" do
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

        # test "successful processing of a non redundant message should delete all keys from the database" do
        {
            my $header = TestLib->header_with_params();
            my $m      = Beetle::Message->new(
                body   => 'foo',
                header => $header,
                queue  => "somequeue",
                store  => $store
            );
            is( $m->expired,   0, 'Message is not expired yet' );
            is( $m->redundant, 0, 'Message is not redundant' );

            my $result = $m->process( sub { } );

            foreach my $key ( $store->keys( $m->msg_id ) ) {
                is( $store->redis->exists($key), 0, "Key $key is not in store anymore" );
            }
        }

        # test "succesful processing of a redundant message twice should delete all keys from the database" do
        {
            my $header = TestLib->header_with_params( redundant => 1 );
            my $m = Beetle::Message->new(
                body   => 'foo',
                header => $header,
                queue  => "somequeue",
                store  => $store
            );
            is( $m->expired,   0, 'Message is not expired yet' );
            is( $m->redundant, 1, 'Message is redundant' );

            $m->process( sub { } );
            $m->process( sub { } );

            foreach my $key ( $store->keys( $m->msg_id ) ) {
                is( $store->redis->exists($key), 0, "Key $key is removed from store after 2nd process call" );
            }
        }
    }
);

done_testing;
