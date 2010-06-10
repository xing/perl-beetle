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

        my $rabbit = sprintf 'localhost:%d', $ports->{rabbit1};
        my $redis  = sprintf 'localhost:%d', $ports->{redis1};

        my $client = Beetle::Client->new(
            config => {
                logger      => '/dev/null',
                redis_hosts => $redis,
                servers     => $rabbit,
            }
        );

        $client->register_queue('testperl');
        $client->register_message('testperl');
        $client->purge('testperl');

        my $exceptions     = 0;
        my $max_exceptions = 10;
        my $handler        = TestLib::Handler::Attempts->new();

        $client->register_handler( testperl => $handler, { exceptions => $max_exceptions, delay => 0 } );

        $client->publish( testperl => 'snafu' );

        my $timer = AnyEvent->timer(
            after => 20,
            cb    => sub {
                $client->stop_listening;
                is( $handler->exceptions, $max_exceptions + 1, 'The handler got called 11 times' );
            },
        );

        $client->listen;
    }
);

done_testing;

