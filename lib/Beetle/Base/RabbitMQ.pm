package Beetle::Base::RabbitMQ;

use Moose;
use namespace::clean -except => 'meta';
use AnyEvent;
use Net::RabbitFoot;
use Data::Dumper;
extends qw(Beetle::Base);

=head1 NAME

Beetle::Base::RabbitMQ - Base class RabbitMQ adaptors

=head1 DESCRIPTION

This is the base class for both RabbitMQ adaptors:

=over 4

=item * Beetle::Bunny

=item * Beetle::AMQP

=back

=cut

has 'anyevent_condvar' => (
    is  => 'rw',
    isa => 'Any',
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
        _get              => 'get',
        _declare_exchange => 'declare_exchange',
        _declare_queue    => 'declare_queue',
        _publish          => 'publish',
        _purge_queue      => 'purge_queue',
        _recover          => 'recover',
    },
    isa  => 'Any',
    lazy => 1,
);

sub add_command_history {
}

sub exchange_declare {
    my $self = shift;
    $self->_connect or die;
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

sub open_channel {
    my ($self) = @_;
    $self->_open_channel;
}

sub queue_bind {
    my $self = shift;
    $self->_connect or die;
    my ( $queue, $exchange, $routing_key ) = @_;
    $self->add_command_history( { queue_bind => \@_ } );
    $self->log->debug( sprintf '[%s:%d] Binding to queue %s on exchange %s using routing key %s',
        $self->host, $self->port, $queue, $exchange, $routing_key );
    $self->_bind_queue(
        exchange    => $exchange,
        queue       => $queue,
        routing_key => $routing_key,
        no_ack      => 0,
    );
}

sub queue_declare {
    my $self = shift;
    $self->_connect or die;
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
    $self->anyevent_condvar->send;
}

sub BUILD {
    my ($self) = @_;
    $self->_init_rf;
}

sub _init_rf {
    my ($self) = @_;
    $self->{rf} = Net::RabbitFoot->new( verbose => $self->config->verbose );
}

sub _connect {
    return 1;
}

BEGIN {
    no warnings 'redefine';

    # TODO: <plu> talk to author of AnyEvent::RabbitMQ how to fix this properly
    *AnyEvent::RabbitMQ::Channel::DESTROY = sub { };
    *AnyEvent::RabbitMQ::DESTROY          = sub { };

    # TODO: <plu> remove this once my patch got accepted
    *AnyEvent::RabbitMQ::Channel::_header = sub {    ## no critic
        my ( $self, $args, $body, ) = @_;

        $args->{weight} ||= 0;

        $self->{connection}->_push_write(
            Net::AMQP::Frame::Header->new(
                weight       => $args->{weight},
                body_size    => length($body),
                header_frame => Net::AMQP::Protocol::Basic::ContentHeader->new(
                    content_type     => 'application/octet-stream',
                    content_encoding => '',
                    headers          => {},
                    delivery_mode    => 1,
                    priority         => 0,
                    correlation_id   => '',

                    # reply_to         => '',
                    expiration => '',
                    message_id => '',
                    timestamp  => time,
                    type       => '',
                    user_id    => '',
                    app_id     => '',
                    cluster_id => '',
                    %$args,
                ),
            ),
            $self->{id},
        );

        return $self;
    };
}

=head1 AUTHOR

See L<Beetle>.

=head1 COPYRIGHT AND LICENSE

See L<Beetle>.

=cut

1;
