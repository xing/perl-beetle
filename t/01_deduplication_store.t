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
    my $o1 = Sub::Override->new( 'Beetle::DeduplicationStore::_new_redis_instance' => sub { shift && return shift } );
    my $o2 = Sub::Override->new( 'Beetle::Config::redis_operation_retries' => sub { return 2 } );

    my $file = "$Bin/etc/redis-master.conf";
    my $store = Beetle::DeduplicationStore->new( hosts => $file );

    is( $store->redis_master_from_master_file, 'from-file:1234',                'Correct master returned' );
    is( $store->lookup_method,                 'redis_master_from_master_file', 'Correct lookup method chosen' );
    is( $store->redis_master_file_changed,     0,                               'File has not been changed' );

    sleep(1);
    system( 'touch', $file );

    is( $store->redis_master_file_changed, 1, 'File has changed' );
    ok( $store->set_current_redis_master_from_master_file, 'Method set_current_redis_master_from_master_file works' );
    is( $store->current_master,                'from-file:1234', 'Master got set from file correctly' );
    is( $store->redis_master_from_master_file, 'from-file:1234', 'Correct master returned' );

    throws_ok {
        $store->with_failover( sub { die "TEST" } );
    }
    qr/NoRedisMaster/, 'Exception works';
}

{
    my $file = "$Bin/etc/empty-redis-master.conf";
    my $store = Beetle::DeduplicationStore->new( hosts => $file );
    is( $store->redis_master_from_master_file, undef, 'The file is empty, so no master is set' );
    is( $store->lookup_method, 'redis_master_from_master_file', 'Correct lookup method chosen' );
    is( $store->set_current_redis_master_from_master_file, undef, 'Current master will not be set' );
    is( $store->current_master,                            undef, 'Current master is still undef' );
}

{
    my $file = "$Bin/etc/multiple-redis-masters.conf";
    my $store = Beetle::DeduplicationStore->new( hosts => $file );
    is( $store->redis_master_from_master_file, undef, 'There is no "system" master in the file, so no master set' );
    is( $store->lookup_method, 'redis_master_from_master_file', 'Correct lookup method chosen' );
    is( $store->set_current_redis_master_from_master_file, undef, 'Current master will not be set' );
    is( $store->current_master,                            undef, 'Current master is still undef' );
}

{
    my $o1 = Sub::Override->new( 'Beetle::DeduplicationStore::_new_redis_instance' => sub { shift && return shift } );

    my $file = "$Bin/etc/multiple-redis-masters.conf";
    my $store = Beetle::DeduplicationStore->new( hosts => $file );
    $store->config->system_name("b");

    is( $store->redis_master_from_master_file, 'host2:1234',                    'Correct master returned' );
    is( $store->lookup_method,                 'redis_master_from_master_file', 'Correct lookup method chosen' );
}

{
    my $o1 = Sub::Override->new( 'Beetle::DeduplicationStore::_new_redis_instance' => sub { shift && return shift } );

    my $file = "$Bin/etc/multiple-redis-masters-with-default.conf";
    my $store = Beetle::DeduplicationStore->new( hosts => $file );

    is( $store->redis_master_from_master_file, 'host3:1234',                    'Correct master returned' );
    is( $store->lookup_method,                 'redis_master_from_master_file', 'Correct lookup method chosen' );
}

done_testing;
