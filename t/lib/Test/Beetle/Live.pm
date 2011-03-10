package    # hide from PAUSE
  Test::Beetle::Live;

use strict;
use warnings;
use FindBin qw( $Bin );
use Test::More;
use Test::TCP::Multi;
use Test::Beetle::Redis ();

use base qw(Exporter);
our @EXPORT = qw(test_beetle_live);

sub test_beetle_live {
    my $cb = shift;

    plan skip_all => 'export BEETLE_LIVE_TEST to enable this test (and maybe BEETLE_START_SERVICES)' unless $ENV{BEETLE_LIVE_TEST};

    # If BEETLE_START_SERVICES is set, we try to start two rabbitmq-server
    # instances as well as two redis-server instances
    if ( $ENV{BEETLE_START_SERVICES} ) {
        chomp( my $rabbitmq_ctl = `which rabbitmqctl` );
        unless ( $rabbitmq_ctl && -e $rabbitmq_ctl && -x _ ) {
            plan skip_all => 'rabbitmqctl not found in your PATH';
        }

        chomp( my $rabbitmq_server = `which rabbitmq-server` );
        unless ( $rabbitmq_server && -e $rabbitmq_server && -x _ ) {
            plan skip_all => 'rabbitmq-server not found in your PATH';
        }

        chomp( my $redis_server = `which redis-server` );
        unless ( $redis_server && -e $redis_server && -x _ ) {
            plan skip_all => 'redis-server not found in your PATH';
        }

        test_multi_tcp(
            server1 => sub {
                my ( $port, $data_hash ) = @_;
                exec "sudo $Bin/../script/start_rabbit perlrabbit1 $port";
            },
            server2 => sub {
                my ( $port, $data_hash ) = @_;
                exec "sudo $Bin/../script/start_rabbit perlrabbit2 $port";
            },
            server3 => sub {
                my ( $port, $data_hash ) = @_;
                my $filename = Test::Beetle::Redis::generate_redis_conf($port);
                exec 'redis-server', $filename;
            },
            server4 => sub {
                my ( $port, $data_hash ) = @_;
                my $slaveof = sprintf( 'slaveof 127.0.0.1 %d', $data_hash->{server3}{port} );
                my $filename = Test::Beetle::Redis::generate_redis_conf( $port, $slaveof );
                exec 'redis-server', $filename;
            },

            client1 => sub {
                my ($data_hash) = @_;
                $cb->(
                    {
                        rabbit1 => $data_hash->{server1}{port},
                        rabbit2 => $data_hash->{server2}{port},
                        redis1  => $data_hash->{server3}{port},
                        redis2  => $data_hash->{server4}{port},
                    },
                    {
                        rabbit1 => $data_hash->{server1}{pid},
                        rabbit2 => $data_hash->{server2}{pid},
                        redis1  => $data_hash->{server3}{pid},
                        redis2  => $data_hash->{server4}{pid},
                    }
                );
                system("sudo $rabbitmq_ctl -n perlrabbit1 stop");
                system("sudo $rabbitmq_ctl -n perlrabbit2 stop");
                kill 9, $data_hash->{server3}{pid};
                kill 9, $data_hash->{server4}{pid};
            },
        );
    }

    # By default we do not start anything, but then we expect two
    # rabbitmq-server instances running on 5672 + 5673 and at least
    # one redis-server instance running on 6379
    else {
        $cb->(
            {
                rabbit1 => 5672,
                rabbit2 => 5673,
                redis1  => 6379,
            },
        );
    }
}

1;
