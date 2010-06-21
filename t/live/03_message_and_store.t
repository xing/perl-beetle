use strict;
use warnings;
use Test::More;
use Sub::Override;

use FindBin qw( $Bin );
use lib ( "$Bin/../lib", "$Bin/../../lib" );
use Test::Beetle;
use Test::Beetle::Redis;

BEGIN {
    use_ok('Beetle::Message');
    use_ok('Beetle::Handler');
}

test_redis(
    sub {
        my $store = shift;

        my $empty_handler = Beetle::Handler->create( sub { } );

        # test "should be able to extract msg_id from any key" do
        {
            $store->flushdb;
            my $header = Test::Beetle->header_with_params();
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
            my $override = Sub::Override->new( 'Beetle::Config::gc_threshold' => sub { return 0; } );
            my $header = Test::Beetle->header_with_params( ttl => 0 );
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
            my $override = Sub::Override->new( 'Beetle::Config::gc_threshold' => sub { return 0; } );
            my $header = Test::Beetle->header_with_params( ttl => 60 );
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
            my $header = Test::Beetle->header_with_params();
            my $m      = Beetle::Message->new(
                body   => 'foo',
                header => $header,
                queue  => "somequeue",
                store  => $store
            );
            is( $m->expired,   0, 'Message is not expired yet' );
            is( $m->redundant, 0, 'Message is not redundant' );

            my $result = $m->process($empty_handler);

            foreach my $key ( $store->keys( $m->msg_id ) ) {
                is( $store->redis->exists($key), 0, "Key $key is not in store anymore" );
            }
        }

        # test "succesful processing of a redundant message twice should delete all keys from the database" do
        {
            my $header = Test::Beetle->header_with_params( redundant => 1 );
            my $m = Beetle::Message->new(
                body   => 'foo',
                header => $header,
                queue  => "somequeue",
                store  => $store
            );
            is( $m->expired,   0, 'Message is not expired yet' );
            is( $m->redundant, 1, 'Message is redundant' );

            $m->process($empty_handler);
            $m->process($empty_handler);

            foreach my $key ( $store->keys( $m->msg_id ) ) {
                is( $store->redis->exists($key), 0, "Key $key is removed from store after 2nd process call" );
            }
        }

        # test "successful processing of a redundant message once should insert all but the delay key and the
        # exception count key into the database" do
        {
            my $header = Test::Beetle->header_with_params( redundant => 1 );
            my $m = Beetle::Message->new(
                body   => 'foo',
                header => $header,
                queue  => "somequeue",
                store  => $store
            );
            is( $m->expired,   0, 'Message is not expired yet' );
            is( $m->redundant, 1, 'Message is redundant' );

            $m->process($empty_handler);

            is( $store->exists( $m->msg_id => 'status' ),     1, 'Key status exists for msg' );
            is( $store->exists( $m->msg_id => 'expires' ),    1, 'Key expires exists for msg' );
            is( $store->exists( $m->msg_id => 'attempts' ),   1, 'Key attempts exists for msg' );
            is( $store->exists( $m->msg_id => 'timeout' ),    1, 'Key timeout exists for msg' );
            is( $store->exists( $m->msg_id => 'ack_count' ),  1, 'Key ack_count exists for msg' );
            is( $store->exists( $m->msg_id => 'delay' ),      0, 'Key delay does not exist for msg' );
            is( $store->exists( $m->msg_id => 'exceptions' ), 0, 'Key exceptions does not exist for msg' );
        }

        # test "an expired message should be acked without calling the handler" do
        {
            my $header = Test::Beetle->header_with_params( ttl => -1 );
            my $m = Beetle::Message->new(
                body   => 'foo',
                header => $header,
                queue  => "somequeue",
                store  => $store
            );
            is( $m->expired, 1, 'Message is expired' );
            my $processed = 0;
            $m->process( sub { $processed = 1; } );
            is( $processed, 0, 'Expired message did not get processed' );
        }

        # test "a delayed message should not be acked and the handler should not be called" do
        {
            my $header = Test::Beetle->header_with_params();
            my $m      = Beetle::Message->new(
                attempts => 2,
                body     => 'foo',
                header   => $header,
                queue    => "somequeue",
                store    => $store,
            );
            $m->set_delay;
            is( $m->key_exists, 0, 'Key did not exist yet' );
            is( $m->delayed,    1, 'Message is delayed' );
            my $processed = 0;
            $m->process( sub { $processed = 1; } );
            is( $processed, 0, 'Delayed message did not get processed' );
        }

        # test "acking a non redundant message should remove the ack_count key" do
        {
            my $header = Test::Beetle->header_with_params();
            my $m      = Beetle::Message->new(
                body   => 'foo',
                header => $header,
                queue  => "somequeue",
                store  => $store,
            );
            $m->process($empty_handler);
            is( $m->redundant, 0, 'Message is not redundant' );
            is( $store->exists( $m->msg_id, 'ack_count' ), 0, 'The key ack_count does not exist in store' );
        }

        # test "a redundant message should be acked after calling the handler" do
        {

            # TODO: <plu> hmm I think this test is crap. Talk to rubys about it.
            my $header = Test::Beetle->header_with_params( redundant => 1 );
            my $m = Beetle::Message->new(
                body   => 'foo',
                header => $header,
                queue  => "somequeue",
                store  => $store,
            );
            is( $m->redundant, 1, 'Message is redundant' );
            $m->process($empty_handler);
        }

        # test "acking a redundant message should increment the ack_count key" do
        {
            my $header = Test::Beetle->header_with_params( redundant => 1 );
            my $m = Beetle::Message->new(
                body   => 'foo',
                header => $header,
                queue  => "somequeue",
                store  => $store,
            );
            is( $store->get( $m->msg_id => 'ack_count' ), undef, 'The key ack_count is undef in store' );
            $m->process($empty_handler);
            is( $m->redundant, 1, 'Message is redundant' );
            is( $store->get( $m->msg_id => 'ack_count' ), 1, 'The key ack_count is 1 after processing the message' );
        }

        # test "acking a redundant message twice should remove the ack_count key" do
        {
            my $header = Test::Beetle->header_with_params( redundant => 1 );
            my $m = Beetle::Message->new(
                body   => 'foo',
                header => $header,
                queue  => "somequeue",
                store  => $store,
            );
            is( $store->get( $m->msg_id => 'ack_count' ), undef, 'The key ack_count is undef in store' );
            $m->process($empty_handler);
            $m->process($empty_handler);
            is( $m->redundant, 1, 'Message is redundant' );
            is( $store->exists( $m->msg_id, 'ack_count' ),
                0, 'The key ack_count does not exist in store after processing twice' );
        }

        {
            my $header = Test::Beetle->header_with_params();
            my $m      = Beetle::Message->new(
                body   => 'foo',
                header => $header,
                queue  => "queue",
                store  => $store,
            );
            $m->aquire_mutex;
            $m->aquire_mutex;
        }
    }
);

done_testing;
