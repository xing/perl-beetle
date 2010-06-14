use strict;
use warnings;
use Test::More;

use FindBin qw( $Bin );
use lib ( "$Bin/lib", "$Bin/../lib" );
use Test::Beetle;

BEGIN {
    use_ok('Beetle::Message');
}

{
    my $m = Beetle::Message->new( queue => "queue", header => Test::Beetle->header_with_params(), body => 'foo' );
    is(
        $m->format_version(),
        $Beetle::Message::FORMAT_VERSION,
        'a message should encode/decode the message format version correctly'
    );
}

{
    my $header = Test::Beetle->header_with_params( redundant => 1 );
    my $m = Beetle::Message->new( queue => "queue", header => $header, body => 'foo' );
    is( $m->redundant(), 1, 'a redundantly encoded message should have the redundant flag set on delivery' );
}

{
    no warnings 'redefine';
    *Beetle::Message::now = sub { return 25; };
    my $header = Test::Beetle->header_with_params( ttl => 17 );
    my $m = Beetle::Message->new( queue => "queue", header => $header, body => 'foo' );
    is( $m->expires_at, 42, 'encoding a message with a specfied time to live should set an expiration time' );
}

{
    no warnings 'redefine';
    *Beetle::Message::now = sub { return 1; };
    my $header = Test::Beetle->header_with_params();
    my $m = Beetle::Message->new( queue => "queue", header => $header, body => 'foo' );
    is(
        $m->expires_at,
        1 + $Beetle::Message::DEFAULT_TTL,
        'encoding a message should set the default expiration date if none is provided in the call to encode'
    );
}

{
    my $key = 'fookey';
    my $o   = Beetle::Message->publishing_options(
        immediate  => 1,
        key        => $key,
        mandatory  => 1,
        persistent => 1,
        redundant  => 1,
    );
    is(
        exists( $o->{mandatory} ) => 1,
        'the publishing options should include both the beetle headers and the amqp params #1'
    );
    is(
        exists( $o->{immediate} ) => 1,
        'the publishing options should include both the beetle headers and the amqp params #2'
    );
    is(
        exists( $o->{persistent} ) => 1,
        'the publishing options should include both the beetle headers and the amqp params #3'
    );
    is(
        $o->{key} => $key,
        'the publishing options should include both the beetle headers and the amqp params #4'
    );
    is(
        $o->{headers}{flags} => 1,
        'the publishing options should include both the beetle headers and the amqp params #5'
    );
}

{
    my $o = Beetle::Message->publishing_options(
        redundant => 1,
        mandatory => 1,
        bogus     => 1,
    );
    is(
        defined( $o->{mandatory} ) => 1,
        'the publishing options should silently ignore other parameters than the valid publishing keys #1'
    );
    isnt(
        exists( $o->{bogus} ) => 1,
        'the publishing options should silently ignore other parameters than the valid publishing keys #2'
    );
    is(
        $o->{headers}{flags} => 1,
        'the publishing options should silently ignore other parameters than the valid publishing keys #3'
    );
}

{
    my $u1 = Beetle::Message->generate_uuid;
    my $u2 = Beetle::Message->generate_uuid;
    isnt( $u1 => $u2, 'generate_uuid creates a new UUID on each call' );    # TODO: <plu> not sure if that is correct
}

{
    my $uuid = 'wadduyouwantfromme';
    no warnings 'redefine';
    *Beetle::Message::generate_uuid = sub { return $uuid; };
    my $o = Beetle::Message->publishing_options( redundant => 1, );
    is(
        $o->{message_id} => $uuid,
        'the publishing options for a redundant message should include the uuid'
    );
}

done_testing;
