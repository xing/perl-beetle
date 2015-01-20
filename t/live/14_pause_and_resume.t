use strict;
use warnings;
use Test::More;

use FindBin qw( $Bin );
use lib ( "$Bin/../lib", "$Bin/../../lib" );
use Beetle::Client;
use Test::Beetle;
use Test::Beetle::Live;

test_beetle_live(
    sub {
        my $ports = shift;

        my $rabbit = sprintf 'localhost:%d localhost:%d', $ports->{rabbit1}, $ports->{rabbit2};
        my $redis  = sprintf 'localhost:%d', $ports->{redis1};

        my (@timers, $paused);

        my $client = Beetle::Client->new(
            config => {
                logger      => '/dev/null',
                redis_hosts => $redis,
                servers     => $rabbit,
            }
        );

        for my $queue (qw(testperl testperl_pause)) {
            $client->register_queue($queue);
            $client->register_message($queue => { redundant => 1 });
            $client->purge($queue);
        }

        $client->register_handler( testperl => sub {
            my $msg = shift;

            # fist message on testperl: pause subscription on testperl_pause
            # and publish a message to each queue
            if ($msg->{body} == 1) {
                push @timers,
                    AnyEvent->timer(after => 1, cb => sub {
                        $client->pause_listening(['testperl_pause']);
                        $paused = 1;
                    }),
                    AnyEvent->timer(after => 2, cb => sub {
                        $client->publish( testperl => 2 );
                        $client->publish( testperl_pause => 2 );
                        ok($paused, 'paused subscription; published more messages');
                    });
            }
            # second message on testperl: resume subscription on testperl_pause
            elsif ($msg->{body} == 2) {
                push @timers,
                    AnyEvent->timer(after => 1, cb => sub {
                        ok($paused, 'got msg on active subscription; resuming subscription');
                        $paused = 0;
                        $client->resume_listening(['testperl_pause']);
                    });
            }
        });

        $client->register_handler( testperl_pause => sub {
            my $msg = shift;

            if ($msg->{body} == 1) {
                ok( !$paused, 'got first msg before pausing subscription' );
            }
            elsif ($msg->{body} == 2) {
                ok( !$paused, 'got second msg only after resuming subscription' );
                push @timers,
                    AnyEvent->timer(after => 1, cb => sub {
                        $client->stop_listening();
                    });
            }
        });

        $client->publish( testperl       => 1 );
        $client->publish( testperl_pause => 1 );

        $client->listen;
    }
);

done_testing;
