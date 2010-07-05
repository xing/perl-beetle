use strict;
use warnings;
use Test::Exception;
use Test::MockObject;
use Test::More;
use Sub::Override;

use FindBin qw( $Bin );
use lib ( "$Bin/lib", "$Bin/../lib" );
use Test::Beetle;

BEGIN {
    use_ok('Beetle::DeduplicationStore');
}

{
    my @keys = Beetle::DeduplicationStore->keys('someid');
    is_deeply(
        \@keys,
        [
            'someid:status',   'someid:ack_count',  'someid:timeout', 'someid:delay',
            'someid:attempts', 'someid:exceptions', 'someid:mutex',   'someid:expires'
        ],
        'keys and key method works as expected'
    );
}

{
    my $override = Sub::Override->new(
        'Beetle::DeduplicationStore::_new_redis_instance' => sub {
            my ( $self, $server ) = @_;
            return $server;
        }
    );
    my $file = "$Bin/etc/redis-master.conf";
    my $store = Beetle::DeduplicationStore->new( hosts => $file );
    is( $store->lookup_method,             'redis_master_from_master_file', 'Correct lookup method chosen' );
    is( $store->redis_master_file_changed, 1,                               'The first time this call is true' );
    is( $store->redis_master_file_changed, 0,                               'File has not been changed' );
    sleep(1);
    system( 'touch', $file );
    is( $store->redis_master_file_changed, 1, 'File has changed' );
    ok( $store->set_current_redis_master_from_master_file, 'Method set_current_redis_master_from_master_file works' );
    is( $store->current_master, 'from-file:1234', 'Master got set from file correctly' );
}

# TODO: <plu> add more new tests

done_testing;
