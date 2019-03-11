package Beetle::DeduplicationStore;

use Moose;
use namespace::clean -except => 'meta';
use Redis;
use Carp qw(croak);
extends qw(Beetle::Base);

=head1 NAME

Beetle::DeduplicationStore - Deduplicate messages using Redis

=head1 DESCRIPTION

The deduplication store is used internally by Beetle::Client to store information on
the status of message processing. This includes:

=over 4

=item * how often a message has already been seen by some consumer

=item * whether a message has been processed successfully

=item * how many attempts have been made to execute a message handler for a given message

=item * how long we should wait before trying to execute the message handler after a failure

=item * how many exceptions have been raised during previous execution attempts

=item * how long we should wait before trying to perform the next execution attempt

=item * whether some other process is already trying to execute the message handler

=back

It also provides a method to garbage collect keys for expired messages.

=cut

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

has 'current_master' => (
    is        => 'rw',
    isa       => 'Any',
    predicate => 'has_current_master',
    clearer   => 'clear_current_master',
);

has 'last_time_master_file_changed' => (
    default => 0,
    is      => 'ro',
    isa     => 'Int',
);

has 'lookup_method' => (
    is  => 'ro',
    isa => 'Str',
);

# list of key suffixes to use for storing values in Redis.
my @KEY_SUFFIXES = qw(status ack_count timeout delay attempts exceptions mutex expires);

sub BUILD {
    my ($self) = @_;
    my $hosts = $self->hosts;
    if ( -e $hosts ) {
        $self->{lookup_method} = 'redis_master_from_master_file';
    }
    else {
        $self->{lookup_method} = 'redis_master_from_server_string';
    }
}

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
    $time ||= time();
    my @keys      = $self->redis->keys("msgid:*:expires");
    my $threshold = $time + $self->config->gc_threshold;
    foreach my $key (@keys) {
        my $expires_at = $self->redis->get($key);
        next unless defined $expires_at;
        if ( $expires_at < $threshold ) {
            my $msg_id = $self->msg_id($key);
            $self->redis->del($_) for $self->keys($msg_id);
        }
    }
    return 1;
}

sub with_failover {
    my ( $self, $code ) = @_;

    my $result;
    my $max_attempts = $self->config->redis_operation_retries;

    foreach my $attempt ( 1 .. $max_attempts ) {
        $result = eval { $code->(); };
        last unless $@;
        $self->log->error("Beetle: redis connection error $@");
        # simply throw the current master away on connection errors
        $self->clear_current_master;
        if ( $attempt < $max_attempts ) {
            $self->log->info("Beetle: retrying redis operation");
        }
        else {
            die "NoRedisMaster";
        }
        sleep 1;
    }

    return $result;
}

sub redis {
    my ($self) = @_;
    my $method = $self->lookup_method;
    return $self->$method;
}

sub redis_master_from_server_string {
    my ($self) = @_;
    unless ( $self->has_current_master ) {
        $self->current_master($self->_new_redis_instance( $self->hosts ));
    }
    return $self->current_master;
}

sub redis_master_from_master_file {
    my ($self) = @_;
    if ($self->redis_master_file_changed || !$self->has_current_master) {
        $self->set_current_redis_master_from_master_file;
    }
    return $self->current_master;
}

sub redis_master_file_changed {
    my ($self)      = @_;
    my ($mtime)     = ( stat( $self->hosts ) )[9];
    my $last_change = $self->last_time_master_file_changed;
    my $result = $last_change != $mtime ? 1 : 0;
    $self->{last_time_master_file_changed} = $mtime if $result;
    return $result;
}

sub set_current_redis_master_from_master_file {
    my ($self) = @_;
    my $file = $self->hosts;
    my $server;

    open(my $masters, "<", $file) or die "Could not open master file $file: $!";
    while (my $line = <$masters>) {
        chomp $line;
        my @parts = split '/', $line;

        if (@parts == 1) {
            $server = $parts[0]
        }
        elsif (@parts == 2) {
            my ($name, $master) = @parts;
            $server = $master if $name eq $self->config->system_name;
        }
    }
    close $masters;

    if ($server) {
        $self->current_master($self->_new_redis_instance($server));
    }
    else {
        $self->clear_current_master();
    }
}

sub _new_redis_instance {
    my ( $self, $server ) = @_;
    my $redis = Redis->new(server => $server);
    $redis->select($self->db);
    return $redis;
}

__PACKAGE__->meta->make_immutable;

=head1 AUTHOR

See L<Beetle>.

=head1 COPYRIGHT AND LICENSE

See L<Beetle>.

=cut

1;
