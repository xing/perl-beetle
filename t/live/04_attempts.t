use strict;
use warnings;
use Test::More;

use FindBin qw( $Bin );
use lib ( "$Bin/../lib", "$Bin/../../lib" );
use TestLib::Live;
use Beetle::Client;
use TestLib::Handler::Attempts;

test_beetle_live(
    sub {
        my $ports = shift;

        my $rabbit = sprintf 'localhost:%d localhost:%d', $ports->{rabbit1}, $ports->{rabbit2};
        my $redis = sprintf 'localhost:%d', $ports->{redis1};

        my $client = Beetle::Client->new(
            config => {

                # logger      => '/dev/null',
                loglevel    => 'DEBUG',
                redis_hosts => $redis,
                servers     => $rabbit,
            }
        );

        $client->register_queue('testperl');
        $client->purge('testperl');
        $client->register_message('testperl');

        my $exceptions     = 0;
        my $max_exceptions = 10;
        my $handler        = TestLib::Handler::Attempts->new();

        $client->register_handler( testperl => $handler, { exceptions => $max_exceptions, delay => 0 } );

        $client->publish( testperl => 'snafu' );

        my $timer = AnyEvent->timer(
            after => 1,
            cb    => sub {
                $client->stop_listening;

                # is( $N, $k, '3 out of the 6 messages got handled (duplicated removed successfully)' );
            },
        );

        $client->listen;
    }
);

done_testing;

