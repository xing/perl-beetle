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
        $client->purge('testperl');
        $client->register_message('testperl');

        my $message = 'some message';
        my $got     = '';

        $client->register_handler(
            testperl => sub {
                my ($message) = @_;
                $got = $message->body;
            }
        );

        $client->publish( testperl => $message );

        my $timer = AnyEvent->timer(
            after => 1,
            cb    => sub {
                is( $got, $message, "Got message" );
                $client->stop_listening;
            },
        );

        $client->listen;
    }
);

done_testing;
