use Test::More tests => 1;

BEGIN {
  use_ok('Beetle::Message');
}

use FindBin qw( $Bin );
use lib ("$Bin/lib", "$Bin/../lib");
use TestLib;

my $m = Beetle::Message->new( queue => "queue", header => TestLib::header_with_params(), body => 'foo');
is($m->format_version(), $Beetle::Message::FORMAT_VERSION, 'a message should encode/decode the message format version correctly');
