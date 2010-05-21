use Test::More tests => 13;

BEGIN {
    use_ok('Beetle::DeduplicationStore');
}

use strict;
use warnings;
use FindBin qw( $Bin );
use lib ( "$Bin/lib", "$Bin/../lib" );
use TestLib;
use Test::MockObject;
use Test::Exception;

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

    isa_ok( $instances->[0], 'Beetle::Redis' );
    is( $instances->[0]->{server}, 'localhost:1', "Instance no. 1 got correct host:port" );

    isa_ok( $instances->[1], 'Beetle::Redis' );
    is( $instances->[1]->{server}, 'localhost:2', "Instance no. 2 got correct host:port" );

    # Add mockups of Beetle::Redis instances
    $instances->[0] = _create_redis_mockup('slave');
    $instances->[1] = _create_redis_mockup('master');

    is( $instances->[0]->info->{role}, 'slave',  'first instance is slave' );
    is( $instances->[1]->info->{role}, 'master', 'second instance is master' );
}

{
    my $store = Beetle::DeduplicationStore->new( hosts => 'localhost:1, localhost:2' );
    my $instances = $store->redis_instances;

    # Add mockups of Beetle::Redis instances
    $instances->[0] = _create_redis_mockup( 'slave', sub { die "murks"; } );
    $instances->[1] = _create_redis_mockup('master');

    is( $store->redis, $instances->[1], 'searching a redis master should find one even if one cannot be accessed' );
    is( $instances->[1]->info->{role}, 'master', 'second instance is master' );
}

{
    my $store = Beetle::DeduplicationStore->new( hosts => 'localhost:1, localhost:2' );
    my $instances = $store->redis_instances;

    # Add mockups of Beetle::Redis instances
    $instances->[0] = _create_redis_mockup('slave');
    $instances->[1] = _create_redis_mockup('slave');

    throws_ok { $store->redis }
    qr/unable to determine a new master redis instance/,
      'searching a redis master should raise an exception if there is none';
}

{
    my $store = Beetle::DeduplicationStore->new( hosts => 'localhost:1, localhost:2' );
    my $instances = $store->redis_instances;

    # Add mockups of Beetle::Redis instances
    $instances->[0] = _create_redis_mockup('master');
    $instances->[1] = _create_redis_mockup('master');

    throws_ok { $store->redis }
    qr/more than one redis master instances/,
      'searching a redis master should raise an exception if there is more than one';
}

sub _create_redis_mockup {
    my ( $type, $info_sub ) = @_;

    $info_sub ||= sub {
        return { role => $type };
    };

    return Test::MockObject->new->mock( 'info' => $info_sub );
}
