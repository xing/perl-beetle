package    # hide from PAUSE
  TestLib::Live;

use strict;
use warnings;
use FindBin qw( $Bin );
use Test::More;
use Test::TCP::Multi;
use TestLib::Redis ();

use base qw(Exporter);
our @EXPORT = qw(test_beetle_live);

sub test_beetle_live {
    my $cb = shift;

    plan skip_all => 'export BEETLE_LIVE_TEST to enable this test' unless $ENV{BEETLE_LIVE_TEST};

    chomp( my $rabbitmq_ctl = `which rabbitmqctl` );
    unless ( $rabbitmq_ctl && -e $rabbitmq_ctl && -x _ ) {
        die 'rabbitmqctl not found in your PATH';
    }

    chomp( my $rabbitmq_server = `which rabbitmq-server` );
    unless ( $rabbitmq_server && -e $rabbitmq_server && -x _ ) {
        die 'rabbitmq-server not found in your PATH';
    }

    chomp( my $redis_server = `which redis-server` );
    unless ( $redis_server && -e $redis_server && -x _ ) {
        die 'redis-server not found in your PATH';
    }

    test_multi_tcp(
        server1 => sub {
            my ( $port, $data_hash ) = @_;
            exec "sudo -n $Bin/../script/start_rabbit rabbit1 $port";
        },
        server2 => sub {
            my ( $port, $data_hash ) = @_;
            exec "sudo -n $Bin/../script/start_rabbit rabbit2 $port";
        },
        server3 => sub {
            my ( $port, $data_hash ) = @_;
            TestLib::Redis::generate_redis_conf($port);
            exec 'redis-server', 't/redis.conf';
        },

        client1 => sub {
            my ($data_hash) = @_;
            $cb->(
                {
                    rabbit1 => $data_hash->{server1}{port},
                    rabbit2 => $data_hash->{server2}{port},
                    redis1  => $data_hash->{server3}{port},
                }
            );
            system("sudo $rabbitmq_ctl -n rabbit1 stop");
            system("sudo $rabbitmq_ctl -n rabbit2 stop");
            kill 9, $data_hash->{server3}{pid};
        },
    );
}

1;
