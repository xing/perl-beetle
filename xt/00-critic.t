use Test::More;
use FindBin;
eval "use Test::Perl::Critic (-profile => '$FindBin::Bin/perlcriticrc')";
plan skip_all => "Test::Perl::Critic required for testing" if $@;
all_critic_ok("$FindBin::Bin/../lib");
