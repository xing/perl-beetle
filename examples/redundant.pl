#!/usr/bin/env perl
use strict;
use warnings;
use FindBin;
use lib "$FindBin::Bin/../lib";
use Beetle::Client;
use Beetle::Handler;
use Data::Dumper;
$Data::Dumper::Sortkeys = 1;

{
    no warnings 'redefine';
    *Beetle::Config::servers = sub { 'localhost:5673 localhost:5672' };
}

my $client = Beetle::Client->new;

$client->register_queue('testperl');
$client->purge('testperl');
$client->register_message( testperl => { redundant => 1 } );

for ( 1 .. 3 ) {
    $client->publish( testperl => "Hello$_" );
}

$client->register_handler(
    testperl => sub {
        my $m = shift;
        warn $m->server;
    }
);

$client->listen;
