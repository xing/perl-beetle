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

        $client->register_exchange('foo');
        $client->register_exchange('bar');

        $client->register_queue( "testperl" => { key => 'foo', exchange => 'foo' } );
        $client->register_binding( "testperl" => { key => 'bar', exchange => 'bar' } );
        $client->purge("testperl");

        $client->register_message('foo');
        $client->register_message('bar');

        my @messages = ();

        $client->register_handler(
            "testperl" => sub {
                my ($message) = @_;
                push @messages, $message->body;
            }
        );

        $client->publish( foo => 'some message from foo exchange' );
        $client->publish( bar => 'some message from bar exchange' );

        my $timer = AnyEvent->timer(
            after => 1,
            cb    => sub {
                my @expected = ( 'some message from bar exchange', 'some message from foo exchange' );
                is_deeply( [ sort @messages ], \@expected, 'Got all messages from both exchanges' );
                $client->stop_listening;
            },
        );

        $client->listen;
    }
);

done_testing;
