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
        ok( $store->flushdb, 'DB flushed' );

        {
            my $header = TestLib::header_with_params();
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
            no warnings 'redefine';
            *Beetle::Config::gc_threshold = sub { return 0; };
            *Beetle::Config::logger = sub { '/dev/null' };
            my $header = TestLib::header_with_params( ttl => 0 );
            my $m      = Beetle::Message->new(
                body   => 'foo',
                header => $header,
                queue  => "somequeue",
                store  => $store
            );
            is($m->key_exists, 0, 'Key did not exist yet');
            is($m->key_exists, 1, 'Key exists');
        }

        # test "should be able to garbage collect expired keys" do
        #   Beetle.config.expects(:gc_threshold).returns(0)
        #   header = header_with_params({:ttl => 0})
        #   message = Message.new("somequeue", header, 'foo', :store => @store)
        #   assert !message.key_exists?
        #   assert message.key_exists?
        #   @store.redis.expects(:del).with(@store.keys(message.msg_id))
        #   @store.garbage_collect_keys(Time.now.to_i+1)
        # end
    }
);

done_testing;
