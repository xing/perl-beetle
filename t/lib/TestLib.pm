package    # hide from PAUSE
  TestLib;

use Moose;
use Beetle::Message;
use Test::MockObject;

sub header_with_params {
    my (%opts) = @_;

    my $beetle_headers = Beetle::Message->publishing_options(%opts);

    my $obj = Test::MockObject->new;
    $obj->mock( 'properties' => sub { return $beetle_headers } );

    return $obj;
}

1;
