package    # hide from PAUSE
  TestLib;

use Moose;
use Beetle::Message;
use Test::MockObject;

sub header_with_params {
    my ($package, %opts) = @_;

    my $beetle_headers = Beetle::Message->publishing_options(%opts);

    return Test::MockObject->new->mock( 'properties' => sub { return $beetle_headers } );
}

1;
