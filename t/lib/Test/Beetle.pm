package    # hide from PAUSE
  Test::Beetle;

use Moose;
use Beetle::Message;

BEGIN {
    use Beetle::Config;
    no warnings 'redefine';
    if ( $ENV{BEETLE_DEBUG_TEST} ) {
        *Beetle::Config::logger   = sub { 'STDERR' };
        *Beetle::Config::loglevel = sub { 'DEBUG' };
    }
    else {
        *Beetle::Config::logger = sub { '/dev/null' };
    }
}

sub header_with_params {
    my ( $package, %opts ) = @_;

    my $beetle_headers = Beetle::Message->publishing_options(%opts);

    return $beetle_headers;
}

1;
