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

        my $text    = 'some message';
        my $got_msg = '';

        $client->register_handler(
            testperl => sub {
                my ($message) = @_;
                $got_msg = $message;
            }
        );

        $client->publish( testperl => $text );

        my $timer = AnyEvent->timer(
            after => 1,
            cb    => sub {
                is( $got_msg->attempts_limit,   1,          "Attempts limit" );
                is( $got_msg->body,             $text,      "Message body" );
                is( $got_msg->delay,            10,         "Delay" );
                is( $got_msg->exceptions_limit, 0,          "Exceptions limit" );
                is( $got_msg->flags,            0,          "Flags" );
                is( $got_msg->format_version,   1,          "Format version" );
                is( $got_msg->queue,            'testperl', "Message queue" );
                is( $got_msg->timeout,          300,        "Message timeout" );

                is( $got_msg->header->content_type,  'application/octet-stream', "Header content type" );
                is( $got_msg->header->delivery_mode, 2,                          "Header delivery mode" );
                is( $got_msg->header->priority,      0,                          "Header priority" );

                is( $got_msg->header->{headers}{flags},          0, "Header flags" );
                is( $got_msg->header->{headers}{format_version}, 1, "Header format version" );
                like( $got_msg->header->{headers}{expires_at}, qr/^\d+$/, "Header expires at" );

                $client->stop_listening;
            },
        );

        $client->listen;
    }
);

done_testing;
