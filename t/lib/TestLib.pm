package    # hide from PAUSE
  TestLib;

use Moose;
use Beetle::Message;

sub header_with_params {
    my ( $package, %opts ) = @_;

    my $beetle_headers = Beetle::Message->publishing_options(%opts);

    return $beetle_headers;
}

1;
