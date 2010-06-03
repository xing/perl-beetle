use strict;
use warnings;
use Test::Exception;
use Test::More;

use FindBin qw( $Bin );
use lib ( "$Bin/lib", "$Bin/../lib" );
use TestLib;

BEGIN {
    use_ok('Beetle::Client');
}

{
    my $client   = Beetle::Client->new( config => {} );
    my $got      = $client->config;
    my $expected = {
        'gc_threshold' => 259200,
        'logger'       => 'STDERR',
        'loglayout'    => '[%d] [%p] (%C:%L) %m%n',
        'loglevel'     => 'DEBUG',
        'password'     => 'guest',
        'redis_db'     => 4,
        'redis_hosts'  => 'localhost:6379',
        'servers'      => 'localhost:5672',
        'user'         => 'guest',
        'vhost'        => '/',
    };
    is_deeply( $got, $expected, 'Empty config hashref uses default' );
}

{
    my $expected = {
        'gc_threshold' => 123,
        'logger'       => '/dev/null',
        'loglayout'    => '%m%n',
        'loglevel'     => 'INFO',
        'password'     => 'secret',
        'redis_db'     => 1,
        'redis_hosts'  => 'somehost:6379',
        'servers'      => 'otherhost:5672',
        'user'         => 'me',
        'vhost'        => '/foo',
    };
    my $client = Beetle::Client->new( config => $expected );
    my $got = $client->config;
    is_deeply( $got, $expected, 'Custom config works' );
}

{
    my $expected = {
        'gc_threshold' => 456,
        'logger'       => '/dev/zero',
        'loglayout'    => 'FOO: %m%n',
        'loglevel'     => 'WARN',
        'password'     => 'secret123',
        'redis_db'     => 2,
        'redis_hosts'  => 'somehost:123',
        'servers'      => 'otherhost:456',
        'user'         => 'admin',
        'vhost'        => '/bar',
    };
    my $client = Beetle::Client->new( configfile => "$Bin/etc/config.pl" );
    my $got = $client->config;
    delete $got->{configfile};
    is_deeply( $got, $expected, 'Custom config from file works' );
}

done_testing;
