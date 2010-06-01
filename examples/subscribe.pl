#!/usr/bin/env perl
use strict;
use warnings;
use FindBin;
use lib "$FindBin::Bin/../lib";
use Beetle::Client;
use Beetle::Handler;
use Data::Dumper;
$Data::Dumper::Sortkeys = 1;

my $client = Beetle::Client->new;

$client->register_queue('testperl');
$client->register_message( testperl => { redundant => 0 } );

my $handler = Beetle::Handler->create(
    sub {
        my ($message) = @_;
        warn "PROCESSING MESSAGE...";
        use Data::Dumper;
        $Data::Dumper::Sortkeys = 1;
        warn Dumper $message;
    }
);

$client->register_handler( testperl => $handler );

$client->listen;
