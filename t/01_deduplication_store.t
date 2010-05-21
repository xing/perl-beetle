use Test::More tests => 9;

BEGIN {
    use_ok('Beetle::DeduplicationStore');
}

use FindBin qw( $Bin );
use lib ( "$Bin/lib", "$Bin/../lib" );
use TestLib;

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
    is( scalar(@$instances), 2, 'got two Redis instances' );
    for ( 1 .. 2 ) {
        my $instance = shift @$instances;
        isa_ok( $instance, 'AnyEvent::Redis' );
        is( $instance->{host}, 'localhost', "Instance no. $_ got correct host" );    # TODO:
        is( $instance->{port}, $_,          "Instance no. $_ got correct port" );
    }
}
