package    # hide from PAUSE
  Test::Beetle::Redis;

use strict;
use warnings;
use Test::More;
use Test::TCP;
use Beetle::DeduplicationStore;

use base qw(Exporter);
our @EXPORT = qw(test_redis);

sub test_redis {
    my $cb = shift;

    chomp( my $redis_server = `which redis-server` );
    unless ( $redis_server && -e $redis_server && -x _ ) {
        plan skip_all => 'redis-server not found in your PATH';
    }

    test_tcp(
        server => sub {
            my $port = shift;
            generate_redis_conf($port);
            exec "redis-server", "t/redis.conf";
        },
        client => sub {
            my $port = shift;
            my $store = Beetle::DeduplicationStore->new( hosts => "127.0.0.1:$port" );
            $cb->($store);
        },
    );
}

sub generate_redis_conf {
    my $port    = shift;
    my $slaveof = shift || 0;
    my $dir     = $FindBin::Bin;

    my $filename = 't/redis.conf';
    $filename .= "-slave" if $slaveof;

    open my $in, "<", "t/redis.conf.template" or die $!;
    open my $out, ">", $filename or die $!;

    while (<$in>) {
        s/__PORT__/$port/;
        s/__DIR__/$dir/;
        if ($slaveof) {
            s/__SLAVEOF__/$slaveof/;
        }
        else {
            s/__SLAVEOF__//;
        }
        print $out $_;
    }

    return $filename;
}

END {
    unlink "t/redis.conf";
    unlink "t/redis.conf-slave" if -e "t/redis.conf-slave";
}

1;
