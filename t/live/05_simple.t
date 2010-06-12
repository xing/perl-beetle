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

        my $text     = 'some message';
        my @messages = ();

        $client->register_handler(
            testperl => sub {
                my ($message) = @_;
                push @messages, $message;
            }
        );

        $client->publish( testperl => $text );
        $client->publish( testperl => $text, { key => 'testperl' } );

        my $timer = AnyEvent->timer(
            after => 1,
            cb    => sub {
                for ( 0 .. 1 ) {
                    is( $messages[$_]->attempts_limit,   1,          "Attempts limit" );
                    is( $messages[$_]->body,             $text,      "Message body" );
                    is( $messages[$_]->delay,            10,         "Delay" );
                    is( $messages[$_]->exceptions_limit, 0,          "Exceptions limit" );
                    is( $messages[$_]->flags,            0,          "Flags" );
                    is( $messages[$_]->format_version,   1,          "Format version" );
                    is( $messages[$_]->queue,            'testperl', "Message queue" );
                    is( $messages[$_]->timeout,          300,        "Message timeout" );

                    is( $messages[$_]->header->content_type,  'application/octet-stream', "Header content type" );
                    is( $messages[$_]->header->delivery_mode, 2,                          "Header delivery mode" );
                    is( $messages[$_]->header->priority,      0,                          "Header priority" );

                    is( $messages[$_]->header->{headers}{flags},          0, "Header flags" );
                    is( $messages[$_]->header->{headers}{format_version}, 1, "Header format version" );
                    like( $messages[$_]->header->{headers}{expires_at}, qr/^\d+$/, "Header expires at" );
                }

                $client->stop_listening;
            },
        );

        $client->listen;
    }
);

done_testing;
