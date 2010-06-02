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
    *Beetle::Config::servers = [qw(localhost:5672 localhost:5673)]
}

my $client = Beetle::Client->new;

$client->register_queue('testperl');
$client->purge('testperl');
$client->register_message( testperl => { redundant => 1 } );

for ( 1 .. 1 ) {
    $client->publish( testperl => "Hello$_" );
}
