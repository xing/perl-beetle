use strict;
use warnings;
use Test::More;

use FindBin qw( $Bin );
use lib ( "$Bin/../lib", "$Bin/../../lib" );
use Beetle::Client;
use Test::Beetle;
use Test::Beetle::Live;
use Test::Beetle::Handler::Timeout;

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

        my $timeout = 2;
        my $handler;
        $handler = Test::Beetle::Handler::Timeout->new(
            on_failure => sub {
                $client->stop_listening;
            },
            on_error => sub {
                my ($exception) = @_;
                like( $exception, qr/Reached timeout after $timeout seconds/, 'Timeout' );
                $client->stop_listening;
            },
            process_duration => 3,
        );

        $client->register_handler(
            testperl => $handler,
            { exceptions => 1, delay => 0, timeout => $timeout },
        );

        $client->publish( testperl => 'snafu' );

        $client->listen;
    }
);

done_testing;

