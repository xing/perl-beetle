use strict;
use warnings;
use Test::More;
use Test::Exception;

use FindBin qw( $Bin );
use lib ( "$Bin/../lib", "$Bin/../../lib" );
use Beetle::Client;
use Test::Beetle;
use Test::Beetle::Live;

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

        $client->register_queue("testperl");
        $client->purge("testperl");
        $client->register_message("testperl");

        my $text = 'testing';

        my @messages;
        $client->register_handler(
            testperl => sub {
                my ($message) = @_;
                push @messages, $message;
                $client->publish( testperl => $text ) if @messages == 1;
            },
        );

        $client->publish( testperl => $text );

        my $timer = AnyEvent->timer(
            after => 1,
            cb    => sub {
                is( scalar(@messages), 2, 'Processed two messages' );
                $client->stop_listening;
            },
        );

        $client->listen;

    }
);

done_testing;
