#!/usr/bin/env perl
use strict;
use warnings;
use FindBin;
use lib "$FindBin::Bin/../lib", "$FindBin::Bin/../t/lib";
use Beetle::Client;
use Beetle::Handler;
use TestLib::Handler::Attempts;
use Data::Dumper;
$Data::Dumper::Sortkeys = 1;

my $client = Beetle::Client->new(
    config => {
        loglevel => 'INFO',
        servers  => 'localhost:5672',
        verbose  => 0,
    },
);

$client->register_queue('testperl');
$client->register_message('testperl');
$client->purge('testperl');

my $exceptions     = 0;
my $max_exceptions = 10;
my $handler        = TestLib::Handler::Attempts->new( client => $client );

$client->register_handler( testperl => $handler, { exceptions => $max_exceptions, delay => 0 } );

$client->publish( testperl => 'snafu' );

$client->listen;
