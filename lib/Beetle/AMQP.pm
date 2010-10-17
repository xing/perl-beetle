package Beetle::AMQP;

use Moose;
use namespace::clean -except => 'meta';
use AnyEvent;
use Coro;
use Net::RabbitFoot;
extends qw(Beetle::Base::RabbitMQ);

=head1 NAME

Beetle::MQ - RabbitMQ adaptor for Beetle::Subscriber

=head1 DESCRIPTION

This is the adaptor to L<Net::RabbitFoot>. Its interface is similar to the
Ruby AMQP client called C<< AMQP >>: http://github.com/tmm1/amqp
So the Beetle code using this adaptor can be closer to the Ruby Beetle
implementation.

=cut

has '_subscriptions' => (
    default => sub { return {}; },
    handles => {
        set_subscription => 'set',
        has_subscription => 'exists'
    },
    is     => 'ro',
    isa    => 'HashRef',
    traits => [qw(Hash)],
);

has '_reconnect_timer' => (
    is  => 'ro',
    isa => 'ArrayRef',
);

has '_command_history' => (
    default => sub { return []; },
    handles => {
        _add_command_history => 'push',
        get_command_history  => 'elements',
    },
    is     => 'ro',
    isa    => 'ArrayRef',
    traits => [qw(Array)],
);

has '_replay' => (
    default => 0,
    is      => 'rw',
    isa     => 'Bool',
);

has '_connect_attempts' => (
    default => 0,
    is      => 'ro',
    isa     => 'Int',
);

after 'BUILD' => sub {
    my ($self) = @_;
    $self->connect;
};

sub add_command_history {
    my ( $self, @args ) = @_;
    $self->_add_command_history(@args) unless $self->_replay;
}

sub ack {
    my ( $self, $options ) = @_;
    $options ||= {};
    $self->_ack(%$options);
}

sub connect {
    my ( $self, $code ) = @_;
    eval {
        $self->rf->connect(
            host     => $self->host,
            port     => $self->port,
            user     => $self->config->user,
            pass     => $self->config->password,
            vhost    => $self->config->vhost,
            on_close => unblock_sub {
                $self->{_reconnect_timer} = AnyEvent->timer(
                    after => 1,
                    cb    => unblock_sub {
                        sleep(1);
                        $self->connect(
                            sub {
                                my $self = shift;
                                $self->{_subscriptions} = {};
                                $self->{_channel}       = $self->_open_channel;
                                $self->_replay_command_history;
                            }
                        );
                    },
                );
            },
        );
    };
    if ($@) {
        $self->log->error($@) if $self->{_connect_attempts}++ % 60 == 0;
        $self->{_reconnect_timer} = AnyEvent->timer(
            after => 1,
            cb    => unblock_sub {
                sleep(1);
                $self->connect($code);
            },
        );
    }
    elsif ($code) {
        $code->($self);
    }
}

sub listen {
    my ($self) = @_;

    my $c = AnyEvent->condvar;
    $self->anyevent_condvar($c);

    # Run the event loop forever
    $c->recv;
}

sub recover {
    my ( $self, $options ) = @_;
    $options ||= {};
    $self->_recover( requeue => 1, %$options );
}

sub subscribe {
    my $self = shift;
    my ( $queue, $callback ) = @_;
    $self->add_command_history( { subscribe => \@_ } );
    my $has_subscription = $self->has_subscription($queue);
    die "Already subscribed to queue $queue" if $has_subscription;
    $self->set_subscription( $queue => 1 );
    $self->log->debug( sprintf '[%s:%d] Subscribing to queue %s', $self->host, $self->port, $queue );
    $self->_consume(
        on_consume => $callback,
        queue      => $queue,
        no_ack     => 0,
    );
}

sub _replay_command_history {
    my ($self) = @_;
    foreach my $command ( $self->get_command_history ) {
        my ( $method, $args ) = %$command;
        $self->_replay(1);
        $self->$method(@$args);
        $self->_replay(0);
    }
}

=head1 AUTHOR

See L<Beetle>.

=head1 COPYRIGHT AND LICENSE

See L<Beetle>.

=cut

1;
