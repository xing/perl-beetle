use strict;
use warnings;
use Test::More;

use FindBin qw( $Bin );
use lib ( "$Bin/lib", "$Bin/../lib" );
use TestLib::Live;
use Beetle::Client;

test_beetle_live(
    sub {
        my $ports = shift;

        my $rabbit = sprintf 'localhost:%d localhost:%d', $ports->{rabbit1}, $ports->{rabbit2};
        my $redis = sprintf 'localhost:%d', $ports->{redis1};

        my $client = Beetle::Client->new(
            config => {
                logger      => '/dev/null',
                redis_hosts => $redis,
                servers     => $rabbit,
            }
        );

        $client->register_queue('testperl');
        $client->purge('testperl');
        $client->register_message( testperl => { redundant => 1 } );

        my $N = 3;
        my $n = 0;
        for ( 1 .. $N ) {
            $n += $client->publish( testperl => "Hello$_" );
        }
        is( $n, 2 * $N, 'published 6 test messages' );

        my $k = 0;

        $client->register_handler(
            testperl => sub {
                $k++;
            }
        );

        my $timer = AnyEvent->timer(
            after => 1,
            cb    => sub {
                $client->stop_listening;
                is( $N, $k, '3 out of the 6 messages got handled (duplicated removed successfully)' );
            },
        );

        $client->listen;
    }
);

done_testing;

