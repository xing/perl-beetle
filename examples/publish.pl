#!/usr/bin/env perl
use strict;
use warnings;
use FindBin;
use lib "$FindBin::Bin/../lib";
use Beetle::Client;
use Data::Dumper;
$Data::Dumper::Sortkeys = 1;

# use Net::RabbitMQ;
# my $mq = Net::RabbitMQ->new();
# $mq->connect( "localhost", { user => "guest", password => "guest" } );
# $mq->channel_open(1);
# $mq->queue_declare( 1, "testperl", { passive => 0, durable => 1, exclusive => 0, auto_delete => 0 } );
# $mq->queue_bind( 1, "testperl", "nr_test_x", "testperl" );
# $mq->publish(
#     1, "testperl", "Magic Payload",
#     { exchange => "testperl" },
# 
#     # {
#     #  content_type => 'text/plain',
#     #  content_encoding => 'none',
#     #  correlation_id => '123',
#     #  reply_to => 'somequeue',
#     #  expiration => 'later',
#     #  message_id => 'ABC',
#     #  type => 'notmytype',
#     #  user_id => 'yoda',
#     #  app_id => 'idd',
#     #  delivery_mode => 1,
#     #  priority => 2,
#     #  timestamp => 1271857990,
#     # },
# );
# $mq->disconnect();

my $client = Beetle::Client->new;

$client->register_queue('testperl');
$client->purge('testperl');
$client->register_message( testperl => { redundant => 0 } );

for ( 1 .. 1 ) {
    $client->publish( testperl => "Hello#$_" );
}
