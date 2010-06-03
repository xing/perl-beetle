#!/usr/bin/env perl
package    # hide from PAUSE
  SomeHandler;

use Moose;
extends qw(Beetle::Handler);

our $COUNTER = 0;

sub process {
    my ($self) = @_;
    my $data = $self->message->body;
    $self->log->info("Adding ${data}");
    $COUNTER += $data;
}

package    # hide from PAUSE
  main;
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
$client->purge('testperl');
$client->deduplication_store->flushdb;

my $counter = 0;
my $handler = SomeHandler->new();

$client->register_handler( 'testperl' => $handler );

my $message_count = 10;
my $published     = 0;

for ( 1 .. $message_count - 1 ) {
    $published += $client->publish( testperl => $_ );
}

warn "published ${published} test messages";

my $timer = AnyEvent->timer(
    after => 1,      # seconds
    cb    => sub {
        warn "result: $SomeHandler::COUNTER";
        $client->stop_listening;
        die "something is fishy" unless $SomeHandler::COUNTER == $message_count * ( $message_count - 1 ) / 2;
    },
);

$client->listen;
