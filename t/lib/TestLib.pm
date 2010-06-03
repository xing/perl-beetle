package    # hide from PAUSE
  TestLib;

use Moose;
use Beetle::Message;

BEGIN {
    # Disable logger in tests
    use Beetle::Config;
    no warnings 'redefine';
    *Beetle::Config::logger = sub { '/dev/null' };
}

sub header_with_params {
    my ( $package, %opts ) = @_;

    my $beetle_headers = Beetle::Message->publishing_options(%opts);

    return $beetle_headers;
}

1;
