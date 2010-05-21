package Beetle::Redis;

use warnings;
use strict;

use IO::Socket::INET;
use Data::Dumper;
use Carp qw/confess/;
use Encode;

our $VERSION = '1.2001';

# This is a copy of Redis-1.2001 with one minor change:
# The connect is not made in the constructor but in the AUTOLOAD method.

sub new {
    my $class = shift;
    my $self  = {@_};
    $self->{debug} ||= $ENV{REDIS_DEBUG};
    bless( $self, $class );
    return $self;
}

my $bulk_command = {
    set       => 1,
    setnx     => 1,
    rpush     => 1,
    lpush     => 1,
    lset      => 1,
    lrem      => 1,
    sadd      => 1,
    srem      => 1,
    sismember => 1,
    echo      => 1,
    getset    => 1,
    smove     => 1,
    zadd      => 1,
    zrem      => 1,
    zscore    => 1,
    zincrby   => 1,
    append    => 1,
};

# we don't want DESTROY to fallback into AUTOLOAD
sub DESTROY { }

our $AUTOLOAD;

sub AUTOLOAD {
    my $self = shift;

    use bytes;

    $self->__connect;

    my $sock = $self->{sock} || die "no server connected";

    my $command = $AUTOLOAD;
    $command =~ s/.*://;

    warn "## $command ", Dumper(@_) if $self->{debug};

    my $send;

    if ( defined $bulk_command->{$command} ) {
        my $value = pop;
        $value = '' if !defined $value;
        $send = uc($command) . ' ' . join( ' ', @_ ) . ' ' . length($value) . "\r\n$value\r\n";
    }
    else {
        $send = uc($command) . ' ' . join( ' ', @_ ) . "\r\n";
    }

    warn ">> $send" if $self->{debug};
    print $sock $send;

    if ( $command eq 'quit' ) {
        close($sock) || die "can't close socket: $!";
        return 1;
    }

    my $result = <$sock> || die "can't read socket: $!";
    Encode::_utf8_on($result); ## no critic
    warn "<< $result" if $self->{debug};
    my $type = substr( $result, 0, 1 );
    $result = substr( $result, 1, -2 );

    if ( $command eq 'info' ) {
        my $hash;
        foreach my $l ( split( /\r\n/, $self->__read_bulk($result) ) ) {
            my ( $n, $v ) = split( /:/, $l, 2 );
            $hash->{$n} = $v;
        }
        return $hash;
    }
    elsif ( $command eq 'keys' ) {
        my $keys = $self->__read_bulk($result);
        return split( /\s/, $keys ) if $keys;
        return;
    }

    if ( $type eq '-' ) {
        confess "[$command] $result";
    }
    elsif ( $type eq '+' ) {
        return $result;
    }
    elsif ( $type eq '$' ) {
        return $self->__read_bulk($result);
    }
    elsif ( $type eq '*' ) {
        return $self->__read_multi_bulk($result);
    }
    elsif ( $type eq ':' ) {
        return $result;    # FIXME check if int?
    }
    else {
        confess "unknown type: $type", $self->__read_line();
    }
}

sub __connect {
    my ( $self, $force ) = @_;
    if ( !$self->{sock} || $force ) {
        $self->{sock} = IO::Socket::INET->new(
            PeerAddr => $self->{server} || $ENV{REDIS_SERVER} || '127.0.0.1:6379',
            Proto => 'tcp',
        ) || die $!;
    }
}

sub __read_bulk {
    my ( $self, $len ) = @_;
    return undef if $len < 0; ## no critic

    my $v;
    if ( $len > 0 ) {
        read( $self->{sock}, $v, $len ) || die $!;
        Encode::_utf8_on($v); ## no critic
        warn "<< ", Dumper($v), $/ if $self->{debug};
    }
    my $crlf;
    read( $self->{sock}, $crlf, 2 );    # skip cr/lf
    return $v;
}

sub __read_multi_bulk {
    my ( $self, $size ) = @_;
    return undef if $size < 0; ## no critic
    my $sock = $self->{sock};

    $size--;

    my @list = ( 0 .. $size );
    foreach ( 0 .. $size ) {
        $list[$_] = $self->__read_bulk( substr( <$sock>, 1, -2 ) );
    }

    warn "## list = ", Dumper(@list) if $self->{debug};
    return @list;
}

1;
