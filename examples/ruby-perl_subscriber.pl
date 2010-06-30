#!/usr/bin/env perl
use strict;
use warnings;
use FindBin;
use lib "$FindBin::Bin/../lib";
use Beetle::Client;
use JSON::XS;
use Data::Dumper;
$Data::Dumper::Sortkeys = 1;

my $json = JSON::XS->new;

my $client = Beetle::Client->new(
    config => {
        servers  => 'localhost:5673 localhost:5672',
        loglevel => 'INFO',
    }
);

$client->register_queue( 'rails_handler'        => { exchange => 'beetle' } );
$client->register_queue( 'rails_result_handler' => { exchange => 'beetle' } );

$client->register_message( result                => { exchange => 'beetle' } );
$client->register_message( redundant_message     => { exchange => 'beetle', redundant => 1 } );
$client->register_message( non_redundant_message => { exchange => 'beetle', redundant => 0 } );

$client->register_binding( rails_result_handler => { key => 'result',                exchange => 'beetle' } );
$client->register_binding( rails_handler        => { key => 'redundant_message',     exchange => 'beetle' } );
$client->register_binding( rails_handler        => { key => 'non_redundant_message', exchange => 'beetle' } );

$client->register_handler(
    rails_handler => sub {
        my ($message) = @_;
        printf "received %s\n", $message->body;
        my $payload  = $json->decode( $message->body );
        my $testcase = $payload->{testcase};
        if (   ( $testcase eq 'redundancy' && !$message->redundant )
            || ( $testcase eq 'non-redundancy' && $message->redundant ) )
        {
            printf "***ERROR*** received wrong message type for testcase %s: %s", $testcase, Dumper($message);
        }
        my $response = { response => $payload->{testcase}, count => $payload->{count} * 10 };
        $client->publish( result => $json->encode($response) );
    }
);

my $timer = AnyEvent->timer(
    after => 10,
    cb    => sub {
        $client->stop_listening;
    },
);

$client->listen;
