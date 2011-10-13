use strict;
use warnings;
use Test::More;

use FindBin qw( $Bin );
use lib ( "$Bin/../lib", "$Bin/../../lib" );
use Beetle::Client;
use Test::Beetle;
use Test::Beetle::Live;
use Test::Beetle::Handler::Attempts;

# Test if the currently running handler will be processed until the end when a
# TERM signal arrives. This test assumes that the handler for the signal will
# simply call "stop_listening" on the client as this is how it should be done
# in scripts running a subscriber.

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

        # Stop listening on the client when a signal arrive. This is how it
        # should be done in scripts running a subscriber.
        $SIG{TERM} = sub { $client->stop_listening() };

        $client->register_queue('testperl');
        $client->register_message('testperl');
        $client->purge('testperl');

        my ($count, $finished);
        $client->register_handler( testperl => sub {
            # send a TERM signal during processing of the second message
            if (++$count == 2) {
                qx/kill $$/;
            }
            $finished++;
        });

        $client->publish( testperl => 'foo' ) for (1..3);

        $client->listen;

        is($finished, 2, 'Finished processing of 2 handlers');
    }
);

done_testing;

