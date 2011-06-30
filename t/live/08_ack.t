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

        # this handler will die when processing the first message so that a
        # reject instead of an ack is triggered
        $client->register_handler(
            testperl => sub {
                my ($message) = @_;
                push @messages, $message;
                die "forced failure on first message" if @messages == 1;
            },
            { exceptions => 1, delay => 0 },
        );

        $client->publish( testperl => $text );

        # we have to wait two seconds as the client will wait 1 sec after the
        # exception
        my $timer = AnyEvent->timer(
            after => 2,
            cb    => sub {
                is( scalar(@messages),                                2, 'Message got processed twice' );
                is( $messages[0]->deliver->method_frame->redelivered, 0, 'Message #0 is not redelivered' );
                is( $messages[1]->deliver->method_frame->redelivered, 1, 'Message #1 is redelivered' );

                $client->stop_listening;
            },
        );

        $client->listen;
    }
);

done_testing;
