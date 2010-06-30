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

$client->purge('rails_handler');
$client->purge('rails_result_handler');

my $redundant_messages_received     = {};
my $non_redundant_messages_received = {};

@{$redundant_messages_received}{qw(0 10 20 30 40 50 60 70 80 90)}     = qw(1 1 1 1 1 1 1 1 1 1);
@{$non_redundant_messages_received}{qw(0 10 20 30 40 50 60 70 80 90)} = qw(1 1 1 1 1 1 1 1 1 1);

$client->register_handler(
    rails_result_handler => sub {
        my ($message) = @_;
        my $payload = $json->decode( $message->body );
        my $messages_received =
          $payload->{response} eq 'redundancy' ? $redundant_messages_received : $non_redundant_messages_received;

        printf "***ERROR*** received a response twice or an invalid response: %s", Dumper $payload
          unless delete $messages_received->{ $payload->{count} };
    }
);

for ( 0 .. 9 ) {
    $client->publish( redundant_message     => $json->encode( { testcase => 'redundancy',     count => $_ } ) );
    $client->publish( non_redundant_message => $json->encode( { testcase => 'non-redundancy', count => $_ } ) );
}

$client->publish( redundant_message => $json->encode( { testcase => 'redundancy', count => 11 } ), { ttl => -86400 } );
$client->publish(
    non_redundant_message => $json->encode( { testcase => 'non-redundancy', count => 11 } ),
    { ttl => -86400 }
);

my $timer = AnyEvent->timer(
    after => 5,
    cb    => sub {
        $client->stop_listening;
        if ( keys %$redundant_messages_received || keys %$non_redundant_messages_received ) {
            my $redundant_messages_received_dumper     = Dumper $redundant_messages_received;
            my $non_redundant_messages_received_dumper = Dumper $non_redundant_messages_received;
            print <<EOF
            not all messages received:
            redundant_messages_received # => $redundant_messages_received_dumper
            non_redundant_messages_received # => $non_redundant_messages_received_dumper
EOF
        }
    },
);

$client->listen;

# 10.times do |n|
#   client.publish(:redundant_message, {:testcase => "redundancy", :count => n}.to_json)
#   client.publish(:non_redundant_message, {:testcase => "non-redundancy", :count => n}.to_json)
# end
#
# client.publish(:redundant_message, {:testcase => "redundancy", :count => 11}.to_json, :ttl => -86400)
# client.publish(:non_redundant_message, {:testcase => "non-redundancy", :count => 12}.to_json, :ttl => -86400)
#
# client.listen do
#   EM.add_timer(5) { client.stop_listening }
# end
#
# unless redundant_messages_received.blank? && non_redundant_messages_received.blank?
#   puts <<-EOF
# ########################################################
# not all messages received:
# redundant_messages_received # => #{redundant_messages_received.inspect}
# non_redundant_messages_received # => #{non_redundant_messages_received.inspect}
# ########################################################
# EOF
# end
#
# puts "Finished"
