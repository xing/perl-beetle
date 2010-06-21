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

        for ( 1 .. 2 ) {
            $client->register_queue("testperl$_");
            $client->purge("testperl$_");
            $client->register_message("testperl$_");
        }

        $client->subscriber->bunny->subscribe( testperl1 => sub { } );
        throws_ok {
            $client->subscriber->bunny->subscribe( testperl1 => sub { } );
        }
        qr/Already subscribed to queue testperl/, 'Subscribing the 2nd time throws an error in the bunny';

        $client->register_handler( testperl2 => sub { } );
        $client->subscriber->subscribe('testperl2');
        throws_ok {
            $client->subscriber->subscribe('testperl2');
        }
        qr/Beetle: binding multiple handlers for the same queue isn't possible/,
          'Subscribing the 2nd time throws an error in the subscriber';
    }
);

done_testing;
