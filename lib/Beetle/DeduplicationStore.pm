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
use namespace::clean -except => 'meta';
use Beetle::Redis;
use Carp qw(croak);
extends qw(Beetle::Base);

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
    isa     => 'ArrayRef',
    lazy    => 1,
);

has '_redis' => (
    clearer   => '_clear_redis',
    is        => 'ro',
    isa       => 'Any',
    predicate => '_has_redis',
);

has 'attempts' => (
    default => 120,
    is      => 'ro',
    isa     => 'Int',
);

# list of key suffixes to use for storing values in Redis.
my @KEY_SUFFIXES = qw(status ack_count timeout delay attempts exceptions mutex expires);

# build a Redis key out of a message id and a given suffix
sub key {
    my ( $package, $msg_id, $suffix ) = @_;
    return "${msg_id}:${suffix}";
}

# list of keys which potentially exist in Redis for the given message id
sub keys {
    my ( $package, $msg_id ) = @_;
    return ( map { $package->key( $msg_id, $_ ) } @KEY_SUFFIXES );
}

# get the Redis instance
sub redis {
    my ($self) = @_;
    return $self->_redis if $self->_has_redis;
    $self->{_redis} ||= $self->_find_redis_master;
    return $self->_redis;
}

# extract message id from a given Redis key
sub msg_id {
    my ( $package, $key ) = @_;
    my ($msg_id) = $key =~ /^(msgid:[^:]+:[-0-9A-F]+):.*?$/i;
    return $msg_id;
}

# unconditionally store a key <tt>value></tt> with given <tt>suffix</tt> for given <tt>msg_id</tt>.
sub set {    ## no critic
    my ( $self, $msg_id, $suffix, $value ) = @_;
    my $key = $self->key( $msg_id, $suffix );
    $self->with_failover( sub { $self->redis->set( $key => $value ) } );
}

# store a key <tt>value></tt> with given <tt>suffix</tt> for given <tt>msg_id</tt> if it doesn't exists yet.
sub setnx {
    my ( $self, $msg_id, $suffix, $value ) = @_;
    my $key = $self->key( $msg_id, $suffix );
    $self->with_failover( sub { $self->redis->setnx( $key => $value ) } );
}

# store some key/value pairs if none of the given keys exist.
sub msetnx {
    my ( $self, $msg_id, $values ) = @_;
    my %result = ();
    foreach my $key ( CORE::keys %$values ) {
        my $value = $values->{$key};
        $key = $self->key( $msg_id, $key );
        $result{$key} = $value;
    }
    $self->with_failover( sub { $self->redis->msetnx(%result) } );
}

# increment counter for key with given <tt>suffix</tt> for given <tt>msg_id</tt>. returns an integer.
sub incr {
    my ( $self, $msg_id, $suffix ) = @_;
    my $key = $self->key( $msg_id, $suffix );
    my $value = $self->with_failover( sub { $self->redis->incr($key) } );
    return $value;
}

# retrieve the value with given <tt>suffix</tt> for given <tt>msg_id</tt>. returns a string.
sub get {
    my ( $self, $msg_id, $suffix ) = @_;
    my $key = $self->key( $msg_id, $suffix );
    my $value = $self->with_failover( sub { $self->redis->get($key) } );
    return $value;
}

# delete key with given <tt>suffix</tt> for given <tt>msg_id</tt>.
sub del {
    my ( $self, $msg_id, $suffix ) = @_;
    my $key = $self->key( $msg_id, $suffix );
    $self->with_failover( sub { $self->redis->del($key) } );
}

# delete all keys associated with the given <tt>msg_id</tt>.
sub del_keys {
    my ( $self, $msg_id ) = @_;
    $self->with_failover( sub { $self->redis->del($_) for $self->keys($msg_id) } );
}

# check whether key with given suffix exists for a given <tt>msg_id</tt>.
sub exists {
    my ( $self, $msg_id, $suffix ) = @_;
    my $key = $self->key( $msg_id, $suffix );
    $self->with_failover( sub { $self->redis->exists($key) } );
}

# flush the configured redis database. useful for testing.
sub flushdb {
    my ($self) = @_;
    $self->with_failover( sub { $self->redis->flushdb } );
}

# garbage collect keys in Redis (always assume the worst!)
sub garbage_collect_keys {
    my ( $self, $time ) = @_;
    $time ||= time;
    my @keys      = $self->redis->keys("msgid:*:expires");
    my $threshold = $time + $self->config->gc_threshold;
    foreach my $key (@keys) {
        my $expires_at = $self->redis->get($key);
        if ( $expires_at && $expires_at < $threshold ) {
            my $msg_id = $self->msg_id($key);
            $self->redis->del($_) for $self->keys($msg_id);
        }
    }
    return 1;
}

# performs redis operations by yielding a passed in block, waiting for a new master to
# show up on the network if the operation throws an exception. if a new master doesn't
# appear after 120 seconds, we raise an exception.
sub with_failover {
    my ( $self, $code ) = @_;

    # TODO: <plu> fix logger + exception
    my $result;
    my $max_attempts = $self->attempts;

    foreach my $attempt ( 1 .. $max_attempts ) {
        $result = eval { $code->(); };
        last unless $@;
        $self->log->error("Beetle: redis connection error $@");
        if ( $attempt < $max_attempts ) {
            $self->log->info("Beetle: retrying redis operation");
        }
        else {
            die "NoRedisMaster";
        }
        $self->_clear_redis;
        sleep 1;
    }

    return $result;
}

sub _build_redis_instances {
    my ($self) = @_;

    my @instances = ();

    foreach my $server ( split /[ ,]+/, $self->hosts ) {
        my $instance = Beetle::Redis->new(
            server => $server,
            db     => $self->db,
        );
        push @instances, $instance;
    }

    return \@instances;
}

sub _find_redis_master {
    my ($self) = @_;
    my @masters = ();
    foreach my $redis ( @{ $self->redis_instances } ) {
        my $role = '';
        eval { $role = $redis->info->{role}; };
        if ($@) {
            $self->log->error("Redis error: $@");
            # TODO: <plu> add proper error logging here
        }
        else {
            push @masters, $redis if $role eq 'master';
        }
    }
    croak "unable to determine a new master redis instance" unless scalar @masters;
    croak "more than one redis master instances" if scalar @masters > 1;
    return $masters[0];
}

__PACKAGE__->meta->make_immutable;

1;
