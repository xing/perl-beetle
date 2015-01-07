package Beetle::Publisher;

use Moose;
use namespace::clean -except => 'meta';
use Hash::Merge::Simple qw( merge );
use Beetle::Message;
use Data::Dumper;
extends qw(Beetle::Base::PubSub);

=head1 NAME

Beetle::Publisher - Publish messages

=head1 DESCRIPTION

TODO: <plu> add docs

=cut

our $RECYCLE_DEAD_SERVERS_DELAY = 10;

has 'client' => (
    is       => 'ro',
    isa      => 'Any',
    weak_ref => 1,
);

has 'exchanges_with_bound_queues' => (
    default => sub { {} },
    handles => {
        has_exchanges_with_bound_queues => 'exists',
        set_exchanges_with_bound_queues => 'set',
    },
    is     => 'ro',
    isa    => 'HashRef',
    traits => [qw(Hash)],
);

has 'dead_servers' => (
    clearer => 'clear_dead_servers',
    default => sub { {} },
    handles => {
        all_dead_servers   => 'elements',
        count_dead_servers => 'count',
        has_dead_servers   => 'count',
        remove_dead_server => 'delete',
        set_dead_server    => 'set',
    },
    is     => 'ro',
    isa    => 'HashRef',
    traits => [qw(Hash)],
);

has 'server_index' => (
    default => 0,
    is      => 'ro',
    isa     => 'Num',
    handles => {
        inc_server_index   => 'inc',
        reset_server_index => 'reset',
    },
    traits => ['Counter'],
);

has 'bunnies' => (
    default => sub { {} },
    handles => {
        get_bunny => 'get',
        has_bunny => 'exists',
        set_bunny => 'set',
    },
    is     => 'ro',
    isa    => 'HashRef',
    traits => [qw(Hash)],
);

sub bunny {
    my ($self) = @_;
    my $has_bunny = $self->has_bunny( $self->server );
    $self->set_bunny( $self->server => $self->new_bunny ) unless $has_bunny;
    return $self->get_bunny( $self->server );
}

sub create_exchange {
    my ( $self, $name, $options ) = @_;
    my %rmq_options = %{ $options || {} };
    delete $rmq_options{queues};
    $self->bunny->exchange_declare( $name => \%rmq_options );
    return 1;
}

sub new_bunny {
    my ($self) = @_;
    my $class = $self->config->bunny_class;
    Class::MOP::load_class($class);
    return $class->new(
        config => $self->config,
        host   => $self->current_host,
        port   => $self->current_port,
    );
}

sub publish {
    my ( $self, $message_name, $data, $options ) = @_;
    $options ||= {};

    my $message = $self->client->get_message($message_name);
    $options = merge $message, $options;

    my $exchange_name = delete $options->{exchange};
    delete $options->{queue};

    $self->recycle_dead_servers if $self->has_dead_servers;

    if ( $options->{redundant} ) {
        $self->publish_with_redundancy( $exchange_name, $message_name, $data, $options );
    }
    else {
        $self->publish_with_failover( $exchange_name, $message_name, $data, $options );
    }
}

sub publish_with_failover {
    my ( $self, $exchange_name, $message_name, $data, $options ) = @_;

    my $tries     = $self->count_servers;
    my $published = 0;

    $options = Beetle::Message->publishing_options(%$options);

    for ( 1 .. $tries ) {
        $self->select_next_server;

        $self->log->debug(
            sprintf 'Beetle: trying to send message %s:%s to %s',
            $message_name, $options->{message_id},
            $self->server
        );

        eval {
            $self->bind_queues_for_exchange($exchange_name);
            my $exchange = $self->exchange($exchange_name);
            my $header   = {
                content_type  => 'application/octet-stream',
                delivery_mode => 2,
                headers       => $options->{headers},
                message_id    => $options->{message_id},
                priority      => 0
            };
            $self->bunny->publish( $exchange_name, $options->{key}, $data, $header );
        };
        unless ($@) {
            $published = 1;
            $self->log->debug('Beetle: message sent!');
            last;
        }
        else {
            $self->log->error($@);
        }

        $self->stop_bunny;
        $self->mark_server_dead;
        $self->log->error( sprintf 'Beetle: message could not be delivered: %s', $message_name );
    }

    return $published;
}

sub publish_with_redundancy {
    my ( $self, $exchange_name, $message_name, $data, $options ) = @_;

    my $count_servers = $self->count_servers;

    if ( $count_servers < 2 ) {
        $self->log->error('Beetle: at least two active servers are required for redundant publishing');
        return $self->publish_with_failover( $exchange_name, $message_name, $data, $options );
    }

    my @published = ();

    $options = Beetle::Message->publishing_options(%$options);

    while (1) {
        my $count_servers = $self->count_servers;

        last if scalar(@published) == 2;
        last unless $count_servers;
        last if scalar(@published) == $count_servers;

        $self->select_next_server;
        next if grep $_ eq $self->server, @published;

        $self->log->debug(
            sprintf 'Beetle: trying to send message %s:%s to %s',
            $message_name, $options->{message_id},
            $self->server
        );

        my $header = {
            content_type  => 'application/octet-stream',
            delivery_mode => 2,
            headers       => $options->{headers},
            message_id    => $options->{message_id},
            priority      => 0
        };

        eval {
            $self->bind_queues_for_exchange($exchange_name);
            $self->bunny->publish( $exchange_name, $options->{key}, $data, $header );
        };
        unless ($@) {
            push @published, $self->server;
            $self->log->debug( sprintf 'Beetle: message sent on server %s (%d)!', $self->server, scalar(@published) );
            next;
        }

        $self->stop_bunny;
        $self->mark_server_dead;
    }

    if ( scalar(@published) == 0 ) {
        $self->log->error( sprintf 'Beetle: message could not be delivered: %s', $message_name );
    }
    if ( scalar(@published) == 1 ) {
        $self->log->error('Beetle: failed to send message redundantly');
    }

    return wantarray ? @published : scalar @published;
}

sub purge {
    my ( $self, $queue_name ) = @_;
    $self->each_server(
        sub {
            my $self = shift;
            eval {
                $self->bunny->purge( $self->queue($queue_name) );
            };
        }
    );
}

sub stop_bunny {
    my ($self) = @_;
    delete $self->{bunnies}{ $self->server };
    $self->{_exchanges}{ $self->server } = {};
    $self->{_queues}{ $self->server }    = {};
}

sub stop {
    my ($self) = @_;
    $self->each_server(
        sub {
            my $self = shift;
            $self->stop_bunny;
        }
    );
}

# private

sub recycle_dead_servers {
    my ($self)  = @_;
    my @recycle = ();
    my %servers = $self->all_dead_servers;
    while ( my ( $server, $time ) = each %servers ) {
        if ( time - $time > $RECYCLE_DEAD_SERVERS_DELAY ) {
            push @recycle, $server;
            $self->remove_dead_server($server);
        }
    }
    if (@recycle == 0 && $self->count_servers == 0) {
        my %dead_servers = $self->all_dead_servers;
        foreach my $server (sort { $dead_servers{$a} <=> $dead_servers{$b} } keys %dead_servers) {
            push @recycle, $server;
            $self->remove_dead_server($server);
            last;
        }
    }
    $self->add_server(@recycle);
}

sub mark_server_dead {
    my ($self) = @_;

    # my $exception = $self->bunny->connect_exception || '';
    my $exception = 'UNKNOWN';
    $self->log->info( sprintf 'Beetle: server %s down: %s', $self->server, $exception );

    $self->set_dead_server( $self->server => time );

    my @servers = grep $_ ne $self->server, $self->all_servers;
    $self->{servers} = \@servers;
    $self->{server}  = $servers[ int rand scalar @servers ];
}

sub select_next_server {
    my ($self) = @_;
    unless ( $self->count_servers ) {
        $self->log->error('Beetle: message could not be delivered - no server available');
        return 0;
    }
    $self->inc_server_index;
    my $next   = $self->server_index % $self->count_servers;
    my $server = $self->get_server($next);
    $self->set_current_server($server);
}

sub bind_queues_for_exchange {
    my ( $self, $exchange_name ) = @_;
    return if $self->has_exchanges_with_bound_queues($exchange_name);
    my $exchange = $self->client->get_exchange($exchange_name);
    my $queues   = $exchange->{queues};
    foreach my $queue (@$queues) {
        $self->set_exchanges_with_bound_queues( $exchange_name => 1 );
        $self->queue($queue);
    }
}

sub bind_queue {
    my ( $self, $queue_name, $creation_keys, $exchange_name, $binding_keys ) = @_;
    $self->log->debug( sprintf 'Creating queue with options: %s', Dumper($creation_keys) );
    $self->bunny->queue_declare( $queue_name, $creation_keys );
    $self->log->debug( sprintf 'Binding queue %s to %s with options %s',
        $queue_name, $exchange_name, Dumper($binding_keys) );
    $self->exchange($exchange_name);
    $self->bunny->queue_bind( $queue_name, $exchange_name, $binding_keys->{key} );
}

__PACKAGE__->meta->make_immutable;

=head1 AUTHOR

See L<Beetle>.

=head1 COPYRIGHT AND LICENSE

See L<Beetle>.

=cut

1;
