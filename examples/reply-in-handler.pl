#!/usr/bin/env perl
use strict;
use warnings;
use FindBin;
use lib "$FindBin::Bin/../lib";
use Beetle::Client;
use Beetle::Handler;
use AnyEvent;
use Data::Dumper;
$Data::Dumper::Sortkeys = 1;

my $client = Beetle::Client->new( config => { loglevel => 'INFO' } );

$client->register_queue('testperl');
$client->register_message( testperl => { redundant => 0 } );

my $handler = Beetle::Handler->create(
    sub {
        my ($message) = @_;
        warn "Got message: ". $message->body;
        $client->publish( testperl => "Hello back" ) unless $message->body eq 'Hello back';
    }
);

$client->register_handler( testperl => $handler );

$client->publish( testperl => "Hello" );

my $timer = AnyEvent->timer(
    after => 10,     # seconds
    cb    => sub {
        $client->stop_listening;
    },
);

$client->listen;
