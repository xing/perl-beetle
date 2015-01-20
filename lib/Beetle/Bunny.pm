package Beetle::Bunny;

use Moose;
use namespace::clean -except => 'meta';
use Data::Dumper;
use Net::AMQP::RabbitMQ;
extends qw(Beetle::Base);

=head1 NAME

Beetle::Bunny - RabbitMQ adaptor for Beetle::Publisher

=head1 DESCRIPTION

This is the adaptor to L<Net::AMQP::RabbitMQ>. Its interface is similar to the
Ruby AMQP client called C<< bunny >>: http://github.com/celldee/bunny
So the Beetle code using this adaptor can be closer to the Ruby Beetle
implementation.

=cut

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

has 'mq' => (
    default => sub { Net::AMQP::RabbitMQ->new },
    isa     => 'Net::AMQP::RabbitMQ',
    is      => 'ro',
    lazy    => 1,
);

has 'connect_exception' => (
    clearer   => 'clear_connect_exception',
    is        => 'ro',
    isa       => 'Str',
    predicate => 'has_connect_exception',
);

has '_channel' => (
    default => 1,
    isa     => 'Int',
    is      => 'ro',
);

sub publish {
    my ( $self, $exchange, $routing_key, $body, $options ) = @_;
    $self->_connect or die;
    $options ||= {};
    $self->log->debug( sprintf '[%s:%d] Publishing message %s on exchange %s',
        $self->host, $self->port, $routing_key, $exchange );
    $self->mq->publish($self->_channel, $routing_key, $body, { exchange => $exchange }, $options);
}

sub purge {
    my ( $self, $queue, $options ) = @_;
    $self->_connect or die;
    $options ||= {};
    $self->mq->purge($self->_channel, $queue, %$options );
}

sub queue_declare {
    my $self = shift;
    $self->_connect or die;
    my ( $queue, $options ) = @_;
    $self->log->debug( sprintf '[%s:%d] Declaring queue with options: %s', $self->host, $self->port, Dumper $options);
    $self->mq->queue_declare($self->_channel, $queue, $options);
}

sub exchange_declare {
    my $self = shift;
    $self->_connect or die;
    my ( $exchange, $options ) = @_;
    $options ||= {};
    $self->log->debug( sprintf '[%s:%d] Declaring exchange %s with options: %s', $self->host, $self->port, $exchange, Dumper $options);
    $options->{exchange_type} = delete $options->{type};
    $options->{auto_delete} ||= 0;
    $self->mq->exchange_declare($self->_channel, $exchange, $options);
}

sub queue_bind {
    my $self = shift;
    $self->_connect or die;
    my ( $queue, $exchange, $routing_key ) = @_;
    $self->log->debug( sprintf '[%s:%d] Binding to queue %s on exchange %s using routing key %s', $self->host, $self->port, $queue, $exchange, $routing_key );
    $self->mq->queue_bind($self->_channel, $queue, $exchange, $routing_key);
}

sub _connect {
    my ($self) = @_;
    return 1 if $self->mq->is_connected();
    $self->clear_connect_exception;
    eval {
        $self->mq->connect(
            $self->host,
            {
                user     => $self->config->user,
                password => $self->config->password,
                port     => $self->port,
                vhost    => $self->config->vhost,
            }
        );
        $self->mq->channel_open($self->_channel);
    };
    $self->{connect_exception} = $@;
    return 0 if $@;
    return 1;
}

__PACKAGE__->meta->make_immutable;

=head1 AUTHOR

See L<Beetle>.

=head1 COPYRIGHT AND LICENSE

See L<Beetle>.

=cut

1;
