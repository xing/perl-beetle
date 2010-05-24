#!/usr/bin/env perl
use strict;
use warnings;
use FindBin;
use lib "$FindBin::Bin/../lib";
use Beetle::DeduplicationStore;
my $store = Beetle::DeduplicationStore->new( hosts => "127.0.0.1:6379" );

my ( $id, $sfx, $val ) = qw(message_id suffix value);
$store->set( $id, $sfx, $val );
warn $store->get( $id, $sfx );
