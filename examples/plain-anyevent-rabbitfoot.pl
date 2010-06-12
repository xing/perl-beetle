#!/usr/bin/env perl
use strict;
use warnings;
use Net::RabbitFoot;

my %config = (
    host  => 'localhost',
    user  => 'guest',
    pass  => 'guest',
    vhost => '/',

    # port  => 5672,
);

Net::AMQP::Protocol->load_xml_spec( Net::RabbitFoot::default_amqp_spec() );

my $rf1 = Net::RabbitFoot->new->connect( %config, port => 5672 );
my $rf2 = Net::RabbitFoot->new->connect( %config, port => 5672 );

$rf1->open_channel;
$rf2->open_channel;

$rf1->close;
$rf2->close;
