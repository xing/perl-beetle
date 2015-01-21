use strict;
use warnings;
use Test::More;
use Test::Exception;

use FindBin qw( $Bin );
use lib ( "$Bin/../lib", "$Bin/../../lib" );
use Beetle::Client;
use Test::Beetle;
use Test::Beetle::Live;


# This test is replicating an issue with a publishing subscriber when started
# when there are already messages on the queue. In such a case there was a
# deadlock in the event loop, as the 'publish' in the message handler of the
# subscriber was blocking.

test_beetle_live(
    sub {
        my $ports = shift;

        my $text = 'testing';

        if (my $pid = fork) {

            sleep 1;

            my $client = _get_client($ports);

            my @messages;
            $client->register_handler(
                testperl => sub {
                    my ($message) = @_;
                    push @messages, $message;
                    if (@messages <= 5) {
                        $client->publish( testperl => "handler: " . @messages );
                    }
                },
            );

            my $timer = AnyEvent->timer(
                after => 3,
                cb    => sub {
                    $client->stop_listening;
                    is( scalar(@messages), 7, 'Processed all messages' );
                },
            );

            $client->listen;
            done_testing;
        }
        else {
            my $client = _get_client($ports);

            $client->purge("testperl");
            $client->publish( testperl => $_ ) for (1..2);
        }
    }
);

sub _get_client {
    my ($ports) = @_;

    my $rabbit = sprintf 'localhost:%d', $ports->{rabbit1};
    my $redis  = sprintf 'localhost:%d', $ports->{redis1};
    my $config = {
        logger      => '/dev/null',
        redis_hosts => $redis,
        servers     => $rabbit,
    };

    my $client = Beetle::Client->new( config => $config );
    $client->register_queue("testperl");
    $client->register_message("testperl");

    return $client;
}
