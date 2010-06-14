use strict;
use warnings;
use Test::More;

use FindBin qw( $Bin );
use lib ( "$Bin/../lib", "$Bin/../../lib" );
use Beetle::Client;
use Test::Beetle;
use Test::Beetle::Live;
use Test::Beetle::Handler::Attempts;

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

        $client->register_exchange('testperl');

        for ( 1 .. 2 ) {
            $client->register_queue( "testperl$_" => { key => 'testperl', exchange => 'testperl' } );
            $client->purge("testperl$_");
        }

        $client->register_message( 'testperl' => { key => 'testperl' } );

        my @messages = ();

        $client->register_handler(
            "testperl1" => sub {
                my ($message) = @_;
                push @messages, sprintf( "Q1: Received message from queue: %s", $message->queue );
            }
        );

        $client->register_handler(
            "testperl2" => sub {
                my ($message) = @_;
                push @messages, sprintf( "Q2: Received message from queue: %s", $message->queue );
            }
        );

        $client->publish( testperl => 'some message' );

        my $timer = AnyEvent->timer(
            after => 1,
            cb    => sub {
                my @expected =
                  ( 'Q1: Received message from queue: testperl1', 'Q2: Received message from queue: testperl2', );
                is_deeply( [ sort @messages ], \@expected, 'Got all messages from both queues' );
                $client->stop_listening;
            },
        );

        $client->listen;
    }
);

done_testing;
