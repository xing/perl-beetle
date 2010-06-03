#!/usr/bin/env perl
use strict;
use warnings;
use FindBin;
use lib "$FindBin::Bin/../lib";
use Beetle::Client;
use Beetle::Handler;
use Data::Dumper;
$Data::Dumper::Sortkeys = 1;

my $client = Beetle::Client->new(
    config => {
        servers  => 'localhost:5673 localhost:5672',
        loglevel => 'INFO',
    }
);

$client->register_queue('testperl');
$client->purge('testperl');
$client->register_message( testperl => { redundant => 1 } );

my $N = 3;
my $n = 0;
for ( 1 .. $N ) {
    $n += $client->publish( testperl => "Hello$_" );
}
printf "published %d test messages\n", $n;

my $expected_publish_count = 2 * $N;
if ( $n != $expected_publish_count ) {
    die "could not publish all messages";
}

my $k = 0;

$client->register_handler(
    testperl => sub {
        $k++;
        my $m = shift;
        printf "Received test message from server %s\n", $m->server;
    }
);

my $timer = AnyEvent->timer(
    after => 1,      # seconds
    cb    => sub {
        $client->stop_listening;
        printf "Received %d test messages\n", $k;
        printf "Your setup is borked\n" if $N != $k;
    },
);

$client->listen;
