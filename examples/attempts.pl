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

my $client = Beetle::Client->new( config => { servers => 'localhost:5672' } );

$client->register_queue('testperl');
$client->purge('testperl');
$client->register_message('testperl');

my $exceptions     = 0;
my $max_exceptions = 10;
my $handler        = TestLib::Handler::Attempts->new();

$client->register_handler( testperl => $handler, { exceptions => $max_exceptions, delay => 0 } );

$client->publish( testperl => 'snafu' );

my $timer = AnyEvent->timer(
    after => 10,
    cb    => sub {
        $client->stop_listening;
    },
);

$client->listen;
