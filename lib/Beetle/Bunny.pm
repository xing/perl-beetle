package Beetle::Bunny;

use Moose;
use namespace::clean -except => 'meta';
use Data::Dumper;


=head1 NAME

Beetle::Bunny - RabbitMQ adaptor for Beetle::Publisher

=head1 DESCRIPTION

This is the adaptor to L<Net::RabbitMQ>. Its interface is similar to the
Ruby AMQP client called C<< bunny >>: http://github.com/celldee/bunny
So the Beetle code using this adaptor can be closer to the Ruby Beetle
implementation.

=cut

has 'connect_exception' => (
    clearer   => 'clear_connect_exception',
    is        => 'ro',
    isa       => 'Str',
    predicate => 'has_connect_exception',
);

sub publish {
    my ( $self, $exchange_name, $message_name, $data, $header ) = @_;
    $self->_connect or die;
    $header ||= {};
    my %data = (
        body        => $data,
        exchange    => $exchange_name,
        routing_key => $message_name,
        header      => $header,
        no_ack      => 0,
    );
    $self->log->debug( sprintf '[%s:%d] Publishing message %s on exchange %s',
        $self->host, $self->port, $message_name, $exchange_name );
    $self->_publish(%data);
}

sub purge {
    my ( $self, $queue, $options ) = @_;
    $self->_connect or die;
    $options ||= {};
    $self->_purge_queue( queue => $queue, %$options );
}

sub _connect {
    my ($self) = @_;
    $self->clear_connect_exception;
    eval {
        $self->rf->connect(
            host  => $self->host,
            port  => $self->port,
            user  => $self->config->user,
            pass  => $self->config->password,
            vhost => $self->config->vhost,
        ) unless $self->rf->{_ar}{_is_open};
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
