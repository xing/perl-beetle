use strict;
use warnings;
use Test::More;
use Test::TCP::Multi;

use FindBin qw( $Bin );
use lib ( "$Bin/lib", "$Bin/../lib" );
use TestLib;

BEGIN {
    use_ok('Beetle::Message');
}

chomp( my $rabbitmq_ctl = `which rabbitmqctl` );
unless ( $rabbitmq_ctl && -e $rabbitmq_ctl && -x _ ) {
    plan skip_all => 'rabbitmqctl not found in your PATH';
}

# unless ( system("sudo -n $rabbitmq_ctl status") == 1 ) {
#     plan skip_all => 'rabbitmqctl must be executable via sudo without password';
# }

chomp( my $rabbitmq_server = `which rabbitmq-server` );
unless ( $rabbitmq_server && -e $rabbitmq_server && -x _ ) {
    plan skip_all => 'rabbitmq-server not found in your PATH';
}

test_multi_tcp(
    server1 => sub {
        my ( $port, $data_hash ) = @_;
        if ( system("sudo -n $Bin/script/start_rabbit rabbit1 $port") != 0 ) {
            plan skip_all => 'rabbitmq-server must be executable via sudo without password';
        }
    },

    # server2 => sub {
    #     my ( $port, $data_hash ) = @_;
    #     warn "server2";
    #     exec "redis-server", "t/redis.conf";
    # },
    # server3 => sub {
    #     my ( $port, $data_hash ) = @_;
    #     warn "server3";
    #     exec "redis-server", "t/redis.conf";
    # },
    client1 => sub {
        my ($data_hash) = @_;
        use Data::Dumper;
        $Data::Dumper::Sortkeys = 1;
        warn Dumper $data_hash;
        system("sudo $rabbitmq_ctl -n rabbit1 stop");
    },

    # client2 => sub {
    #     warn "client2";
    #     my ($data_hash) = @_;
    #     use Data::Dumper;
    #     $Data::Dumper::Sortkeys = 1;
    #     warn Dumper $data_hash;
    #
    #     # kill_proc($data_hash->{ your_server_name }->{pid});
    # },
    # client3 => sub {
    #     warn "client3";
    #     my ($data_hash) = @_;
    #     use Data::Dumper;
    #     $Data::Dumper::Sortkeys = 1;
    #     warn Dumper $data_hash;
    #
    #     # kill_proc($data_hash->{ your_server_name }->{pid});
    # },
);

done_testing;

