use Test::More tests => 12;

BEGIN {
    use_ok('Beetle::DeduplicationStore');
}

use strict;
use warnings;
use FindBin qw( $Bin );
use lib ( "$Bin/lib", "$Bin/../lib" );
use TestLib;
use Test::MockObject;

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
    my $store = Beetle::DeduplicationStore->new( hosts => 'localhost:1, localhost:2' );
    my $instances = $store->redis_instances;

    is( scalar(@$instances), 2, 'redis instances should be created for all servers' );

    isa_ok( $instances->[0], 'AnyEvent::Redis' );
    is( $instances->[0]->{host}, 'localhost', "Instance no. 1 got correct host" );
    is( $instances->[0]->{port}, 1,           "Instance no. 1 got correct port" );

    isa_ok( $instances->[1], 'AnyEvent::Redis' );
    is( $instances->[1]->{host}, 'localhost', "Instance no. 2 got correct host" );
    is( $instances->[1]->{port}, 2,           "Instance no. 2 got correct port" );

    # Add mockups of AnyEvent::Redis instances
    $instances->[0] = _create_redis_mockup('slave');
    $instances->[1] = _create_redis_mockup('master');

    is( $instances->[0]->info->recv->{role}, 'slave',  'first instance is slave' );
    is( $instances->[1]->info->recv->{role}, 'master', 'second instance is master' );

    is( $store->redis, $instances->[1], 'searching a redis master should find one if there is one' );

}

sub _create_redis_mockup {
    my ( $type, $sub ) = @_;

    $sub ||= sub {
        return { role => $type };
    };

    return Test::MockObject->new->mock(
        'info' => sub {
            return Test::MockObject->new->mock( 'recv' => $sub );
        }
    );
}
