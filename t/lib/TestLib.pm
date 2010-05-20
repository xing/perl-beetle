package    # hide from PAUSE
  TestLib;

use Moose;
use Beetle::Message;
use TestLib::Header;

# def header_with_params(opts = {})
#   beetle_headers = Beetle::Message.publishing_options(opts)
#   header = mock("header")
#   header.stubs(:properties).returns(beetle_headers)
#   header
# end

sub header_with_params {
    my (%opts) = @_;

    my $beetle_headers = Beetle::Message->publishing_options(%opts);

    return TestLib::Header->new( properties => $beetle_headers );
}

1;
