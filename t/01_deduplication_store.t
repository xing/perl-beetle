use Test::More tests => 2;

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
