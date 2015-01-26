package Beetle::AMQP;

use Moose;
use namespace::clean -except => 'meta';
use AnyEvent;
use Net::RabbitFoot;
use Data::Dumper;
use Coro qw/unblock_sub/;
extends qw(Beetle::Base);

=head1 NAME

Beetle::AMQP - RabbitMQ adaptor for Beetle::Subscriber

=head1 DESCRIPTION

This is the adaptor to L<Net::RabbitFoot>. Its interface is similar to the
Ruby AMQP client called C<< AMQP >>: http://github.com/tmm1/amqp
So the Beetle code using this adaptor can be closer to the Ruby Beetle
implementation.

=cut

has '_subscriptions' => (
    default => sub { return {}; },
    handles => {
        set_subscription    => 'set',
        has_subscription    => 'exists',
        get_subscription    => 'get',
        delete_subscription => 'delete',
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
        _add_command_history        => 'push',
        get_command_history         => 'elements',
        delete_from_command_history => 'delete',
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

has 'anyevent_condvar' => (
    is      => 'rw',
    isa     => 'AnyEvent::CondVar',
    default => sub { AnyEvent->condvar },
    lazy    => 1,
);

has 'host' => (
    is       => 'rw',
    isa      => 'Str',
    required => 1,
);

has 'port' => (
    is       => 'rw',
    isa      => 'Str',
    required => 1,
);

has 'rf' => (
    isa     => 'Any',
    is      => 'ro',
    handles => { _open_channel => 'open_channel', },
);

has '_channel' => (
    default => sub { shift->open_channel },
    handles => {
        _ack              => 'ack',
        _close            => 'close',
        _bind_queue       => 'bind_queue',
        _consume          => 'consume',
        _cancel           => 'cancel',
        _get              => 'get',
        _declare_exchange => 'declare_exchange',
        _declare_queue    => 'declare_queue',
        _recover          => 'recover',
        _reject           => 'reject',
        _qos              => 'qos',
    },
    isa  => 'Any',
    lazy => 1,
);

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
                                $self->{_channel}       = $self->open_channel;
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

    # Run the event loop forever
    $self->anyevent_condvar->recv;
}

sub open_channel {
    my ($self) = @_;
    $self->_open_channel(
        on_close => unblock_sub {
            my ($frame) = @_;
            my $text    = $frame->method_frame->{reply_text};
            my $code    = $frame->method_frame->{reply_code};
            $self->log->error( sprintf '[%s:%d] %s: %s', $self->host, $self->port, $code, $text );
            $self->rf->ar->close;
        }
    );
}

sub recover {
    my ( $self, $options ) = @_;
    $options ||= {};
    $self->_recover( requeue => 1, %$options );
}

sub reject {
    my ( $self, $options ) = @_;
    $options ||= {};
    $self->_reject( requeue => 1, %$options );
}

sub subscribe {
    my $self = shift;
    my ( $queue, $callback ) = @_;

    $self->add_command_history( { subscribe => \@_ } );
    my $has_subscription = $self->has_subscription($queue);
    die "Already subscribed to queue $queue" if $has_subscription;
    $self->log->debug( sprintf '[%s:%d] Subscribing to queue %s', $self->host, $self->port, $queue );
    my $frame = $self->_consume(
        on_consume => $callback,
        queue      => $queue,
        no_ack     => 0,
    );
    my $consumer_tag = $frame->method_frame->consumer_tag;
    $self->set_subscription( $queue => $consumer_tag );
}

sub unsubscribe {
    my ( $self, $queue ) = @_;

    my $consumer_tag = $self->get_subscription($queue);

    # not subscribed
    return
        unless $consumer_tag;

    $self->log->debug( sprintf '[%s:%d] Unsubscribing from queue %s (consumer tag %s)', $self->host, $self->port, $queue, $consumer_tag );

    # remove last subscribe command for this queue from the command history
    my @history = reverse $self->get_command_history();
    for (0..$#history) {
        if (my $subscribe_cmd = $history[$_]->{subscribe}) {
            if ($subscribe_cmd->[0] eq $queue) {
                $self->delete_from_command_history($#history - $_);
                last;
            }
        }
    }

    $self->_cancel(consumer_tag => $consumer_tag);
    $self->delete_subscription( $queue );
}

sub exchange_declare {
    my $self = shift;
    my ( $exchange, $options ) = @_;
    $self->add_command_history( { exchange_declare => \@_ } );
    $options ||= {};
    $self->log->debug( sprintf '[%s:%d] Declaring exchange %s with options: %s', $self->host, $self->port, $exchange, Dumper $options);
    $self->_declare_exchange(
        exchange => $exchange,
        no_ack   => 0,
        %$options
    );
}

sub queue_bind {
    my $self = shift;
    my ( $queue, $exchange, $routing_key ) = @_;
    $self->add_command_history( { queue_bind => \@_ } );
    $self->log->debug( sprintf '[%s:%d] Binding to queue %s on exchange %s using routing key %s', $self->host, $self->port, $queue, $exchange, $routing_key );
    $self->_bind_queue(
        exchange    => $exchange,
        queue       => $queue,
        routing_key => $routing_key,
        no_ack      => 0,
    );
}

sub queue_declare {
    my $self = shift;
    my ( $queue, $options ) = @_;
    $self->add_command_history( { queue_declare => \@_ } );
    $self->log->debug( sprintf '[%s:%d] Declaring queue with options: %s', $self->host, $self->port, Dumper $options);
    $self->_declare_queue(
        no_ack => 0,
        queue  => $queue,
        %$options
    );
}

sub stop {
    my ($self) = @_;
    $self->rf->ar->close;
    $self->anyevent_condvar->send;
}

sub BUILD {
    my ($self) = @_;
    $self->{rf} = Net::RabbitFoot->new( verbose => $self->config->verbose );
    $self->connect;
    $self->_qos( prefetch_count => 1 );
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

BEGIN {
    no warnings 'redefine';

    # TODO: <plu> talk to author of AnyEvent::RabbitMQ how to fix this properly
    *AnyEvent::RabbitMQ::Channel::DESTROY = sub { };
    *AnyEvent::RabbitMQ::DESTROY          = sub { };
}

=head1 AUTHOR

See L<Beetle>.

=head1 COPYRIGHT AND LICENSE

See L<Beetle>.

=cut

1;
