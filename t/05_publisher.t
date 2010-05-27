use Test::More;

BEGIN {
    use_ok('Beetle::Publisher');
    use_ok('Beetle::Client');
}

use strict;
use warnings;
use Test::Exception;

# test "acccessing a bunny for a server which doesn't have one should create it and associate it with the server" do
{
    no warnings 'redefine';
    *Beetle::Publisher::new_bunny = sub { return 42; };
    my $client = Beetle::Client->new;
    my $pub = Beetle::Publisher->new( client => $client );
    is( $pub->bunny,                     42, 'Method new_bunny works as expected' );
    is( $pub->get_bunny( $pub->server ), 42, 'Bunnies got set correctly' );
}

{
    my $client = Beetle::Client->new;
    my $pub = Beetle::Publisher->new( client => $client );
    is_deeply( $pub->{bunnies}, {}, 'initially there should be no bunnies' );
}

{
    my $client = Beetle::Client->new;
    my $pub = Beetle::Publisher->new( client => $client );
    is_deeply( $pub->{dead_servers}, {}, 'initially there should be no dead servers' );
}

done_testing;
