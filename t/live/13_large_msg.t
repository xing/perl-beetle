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
        $client->register_message('testperl');
        $client->purge('testperl');

        $client->register_handler( testperl => sub {
            my $msg = shift;

            my $payload = $msg->{body};

            is(length($payload), 200_000, 'received large message');
            $client->stop_listening();
        });

        $client->publish( testperl => 'a' x 200_000 );

        $client->listen;
    }
);

done_testing;

