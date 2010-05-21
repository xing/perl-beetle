package Beetle::DeduplicationStore;

use Moose;

has 'hosts' => (
    default => 'localhost:6379',
    is      => 'ro',
    isa     => 'Str',
);

has 'db' => (
    default => 4,
    is      => 'ro',
    isa     => 'Int',
);

# list of key suffixes to use for storing values in Redis.
my @KEY_SUFFIXES = ( 'status', 'ack_count', 'timeout', 'delay', 'attempts', 'exceptions', 'mutex', 'expires' );

sub key {
    my ( $package, $msg_id, $suffix ) = @_;
    return "${msg_id}:${suffix}";
}

sub keys {
    my ( $package, $msg_id ) = @_;
    return map { $package->key( $msg_id, $_ ) } @KEY_SUFFIXES;
}

1;
