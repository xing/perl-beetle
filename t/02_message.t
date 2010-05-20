use Test::More tests => 5;

BEGIN {
  use_ok('Beetle::Message');
}

use FindBin qw( $Bin );
use lib ("$Bin/lib", "$Bin/../lib");
use TestLib;

{
    my $m = Beetle::Message->new( queue => "queue", header => TestLib::header_with_params(), body => 'foo');
    is($m->format_version(), $Beetle::Message::FORMAT_VERSION, 'a message should encode/decode the message format version correctly');
}

{
    my $header = TestLib::header_with_params(':redundant' => 1);
    my $m = Beetle::Message->new( queue => "queue", header => $header, body => 'foo');
    is($m->redundant(), 1, 'a redundantly encoded message should have the redundant flag set on delivery');
}

{
    no warnings 'redefine';
    *Beetle::Message::now = sub { return 25; };
    my $header = TestLib::header_with_params(':ttl' => 17);
    my $m = Beetle::Message->new( queue => "queue", header => $header, body => 'foo');
    is($m->expires_at, 42, 'encoding a message with a specfied time to live should set an expiration time');
}

{
    no warnings 'redefine';
    *Beetle::Message::now = sub { return 1; };
    my $header = TestLib::header_with_params();
    my $m = Beetle::Message->new( queue => "queue", header => $header, body => 'foo');
    is($m->expires_at, 1 + $Beetle::Message::DEFAULT_TTL, 'encoding a message should set the default expiration date if none is provided in the call to encode');
}

