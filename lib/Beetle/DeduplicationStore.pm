package Beetle::DeduplicationStore;

# The deduplication store is used internally by Beetle::Client to store information on
# the status of message processing. This includes:
# * how often a message has already been seen by some consumer
# * whether a message has been processed successfully
# * how many attempts have been made to execute a message handler for a given message
# * how long we should wait before trying to execute the message handler after a failure
# * how many exceptions have been raised during previous execution attempts
# * how long we should wait before trying to perform the next execution attempt
# * whether some other process is already trying to execute the message handler
#
# It also provides a method to garbage collect keys for expired messages.

use Moose;
use AnyEvent::Redis;

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

has 'redis_instances' => (
    builder => '_build_redis_instances',
    is      => 'ro',
    isa     => 'ArrayRef[AnyEvent::Redis]',
    lazy    => 1,
);

has 'redis' => (
    builder => '_build_redis',
    is      => 'ro',
    isa     => 'Any', # TODO: <plu> this should be AnyEvent::Redis, but that does not work with the mockups in the tests
    lazy    => 1,
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

sub _build_redis_instances {
    my ($self) = @_;

    my @instances = ();

    foreach my $server ( split /[ ,]+/, $self->hosts ) {
        my ( $host, $port ) = split /:/, $server;
        my $instance = AnyEvent::Redis->new(
            host => $host,
            port => $port,

            # on_error => sub { warn @_ }, # TODO: <plu> do we need that?
        );
        push @instances, $instance;
    }

    return \@instances;
}

sub _build_redis {
    my ($self) = @_;
    my @masters = ();
    foreach my $redis ( @{ $self->redis_instances } ) {
        my $role = '';
        eval { $role = $redis->info->recv->{role}; };
        if ($@) {
            warn $@;    # TODO: <plu> add proper error logging here
        }
        push @masters, $redis if $role eq 'master';
    }
    die "unable to determine a new master redis instance" unless scalar @masters;
    die "more than one redis master instances" if scalar @masters > 1;
    return $masters[0];
}

1;
