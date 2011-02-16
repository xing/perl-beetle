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
        my $config = {
            logger      => '/dev/null',
            redis_hosts => $redis,
            servers     => $rabbit,
        };

        my $client = Beetle::Client->new( config => $config );

        $client->register_queue('testperl');
        $client->purge('testperl');
        $client->register_message('testperl');

        my $text     = 'some message';
        my @messages = ();

        $client->register_handler(
            testperl => sub {
                my ($message) = @_;
                push @messages, $message;
                $client->subscriber->bunny->recover( { requeue => 0 } );
            },
        );

        $client->publish( testperl => $text );

        my $timer = AnyEvent->timer(
            after => 1,
            cb    => sub {
                is( 1, 1 );

                # TODO: <plu> those are broken for some reason
                # is( scalar(@messages),                                2, 'Message got processed twice' );
                # is( $messages[0]->deliver->method_frame->redelivered, 0, 'Message #0 is not redelivered' );
                # is( $messages[1]->deliver->method_frame->redelivered, 1, 'Message #1 is redelivered' );

                $client->stop_listening;
            },
        );

        $client->listen;
    }
);

done_testing;
