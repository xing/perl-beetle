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

        # test "should be able to extract msg_id from any key" do
        #   header = header_with_params({})
        #   message = Message.new("somequeue", header, 'foo', :store => @store)
        #   @store.keys(message.msg_id).each do |key|
        #     assert_equal message.msg_id, @store.msg_id(key)
        #   end
        # end
    }
);

done_testing;
