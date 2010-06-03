use strict;
use warnings;
use Test::Exception;
use Test::More;

use FindBin qw( $Bin );
use lib ( "$Bin/lib", "$Bin/../lib" );
use TestLib;

BEGIN {
    use_ok('Beetle::Base::PubSub');
    use_ok('Beetle::Client');
}

{
    my $client = Beetle::Client->new;
    my $base = Beetle::Base::PubSub->new( client => $client );
    is( $base->{exchanges}, undef, 'initially we should have no exchanges' );
}

{
    my $client = Beetle::Client->new;
    my $base = Beetle::Base::PubSub->new( client => $client );
    is( $base->{queues}, undef, 'initially we should have no queues' );
}

{
    no warnings 'redefine';
    my $client = Beetle::Client->new;
    my $base = Beetle::Base::PubSub->new( client => $client );
    throws_ok { $base->error('message') } qr/message/, 'the error method should raise a beetle error';
}

{
    my $client = Beetle::Client->new;
    my $base = Beetle::Base::PubSub->new( client => $client );
    isnt( $base->get_server(0), undef, 'first server is defined' );
    is( $base->get_server(0), $base->server, 'server should be initialized' );
}

{
    my $client = Beetle::Client->new;
    my $base = Beetle::Base::PubSub->new( client => $client );
    $base->{server} = 'localhost:123';
    is( $base->current_host, 'localhost', 'current_host should return the hostname of the current server' );
}

{
    my $client = Beetle::Client->new;
    my $base = Beetle::Base::PubSub->new( client => $client );
    $base->{server} = 'localhost:123';
    is( $base->current_port, '123', 'current_port should return the port of the current server as an integer' );
}

{
    my $client = Beetle::Client->new;
    my $base = Beetle::Base::PubSub->new( client => $client );
    $base->{server} = 'localhost';
    is( $base->current_port, '5672',
        'current_port should return the default rabbit port if server string does not contain a port' );
}

{
    my $client = Beetle::Client->new;
    my $base   = Beetle::Base::PubSub->new( client => $client );
    my $result = $base->set_current_server('xxx:123');
    is( $result, 'xxx:123', 'set_current_server shoud set the current server' );
}

done_testing;
