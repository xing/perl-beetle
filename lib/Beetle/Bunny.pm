package Beetle::Bunny;

use Moose;
use AnyEvent;
use Net::RabbitFoot;

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

has 'user' => (
    is       => 'rw',
    isa      => 'Str',
    required => 1,
);

has 'pass' => (
    is       => 'rw',
    isa      => 'Str',
    required => 1,
);

has 'vhost' => (
    is       => 'rw',
    isa      => 'Str',
    required => 1,
);

has 'verbose' => (
    default => 0,
    is      => 'rw',
    isa     => 'Bool',
);

has '_mq' => (
    isa        => 'Any',
    lazy_build => 1,
    handles    => { _open_channel => 'open_channel', },
);

has '_channel' => (
    default => sub { shift->_open_channel },
    handles => {
        close             => 'close',
        _bind_queue       => 'bind_queue',
        _consume          => 'consume',
        _declare_exchange => 'declare_exchange',
        _declare_queue    => 'declare_queue',
        _publish          => 'publish',
    },
    isa  => 'Any',
    lazy => 1,
);

sub exchange_declare {
    my ( $self, $exchange, $options ) = @_;
    $options ||= {};
    $self->_declare_exchange(
        exchange => $exchange,
        %$options,
    );
}

sub listen {
    my ($self) = @_;
    my $c = AnyEvent->condvar;

    # Run the event loop forever
    $c->recv;
}

sub publish {
    my ( $self, $exchange_name, $message_name, $data, $header ) = @_;
    $header ||= {};
    my %data = (
        body        => $data,
        exchange    => $exchange_name,
        routing_key => $message_name,
        header      => $header,
    );
    use Data::Dumper;
    $Data::Dumper::Sortkeys=1;
    warn Dumper $header;
    $self->_publish(%data);
}

sub queue_declare {
    my ( $self, $queue, $options ) = @_;
    $options ||= {};
    $self->_declare_queue(
        queue => $queue,
        %$options,
    );
}

sub queue_bind {
    my ( $self, $queue, $exchange, $routing_key ) = @_;
    $self->_bind_queue(
        exchange    => $exchange,
        queue       => $queue,
        routing_key => $routing_key,
    );
}

sub subscribe {
    my ( $self, $queue, $callback ) = @_;
    $self->_consume(
        on_consume => $callback,
        queue      => $queue
    );
}

sub _build__mq {
    my ($self) = @_;
    my $rf = Net::RabbitFoot->new( verbose => $self->verbose );
    $rf->load_xml_spec( Net::RabbitFoot::default_amqp_spec() );
    $rf->connect(
        host  => $self->host,
        port  => $self->port,
        user  => $self->user,
        pass  => $self->pass,
        vhost => $self->vhost,
    );
    return $rf;
}

1;

__END__
use Coro;
use AnyEvent;
use Net::RabbitFoot;
use Method::Signatures::Simple;
use MooseX::Types::LoadableClass qw/ LoadableClass /;
use AnyEvent::Util qw/ fork_call /;
use MooseX::Types::Moose qw/ Object /;
use Data::Dumper;
use JSON qw/ decode_json /;
use state51::Base;
use namespace::autoclean;

my @jobs = qw/
    state51::Job::OrderXLSValidate
/;

method load_all_jobs {
    to_LoadableClass($_) for @jobs;
}

has _mq => (
    isa => Object,
    lazy_build => 1,
    handles => {
        _open_channel => 'open_channel',
    },
);

method _build__mq {
    my $rf = Net::RabbitFoot->new(
#        verbose => 1,
    )->load_xml_spec(
        Net::RabbitFoot::default_amqp_spec(),
    )->connect(
       host => "button",
       port => 5672,
       user => 'guest',
       pass => 'guest',
       vhost => '/',
    );
   return $rf;
}

has _channel => (
    isa => Object,
    lazy => 1,
    default => sub { shift->_open_channel },
    handles => {
        _declare_exchange => 'declare_exchange',
        _declare_queue => 'declare_queue',
        _bind_queue => 'bind_queue',
        _consume => 'consume',
    },
);

method bind_all_queues {
    my %seen;
    foreach my $name (map { $_->exchange_name } @jobs) {
        my $exch_frame = $self->_declare_exchange(
            type => 'topic',
            durable => 1,
            exchange => $name,
        )->method_frame;
        die Dumper($exch_frame) unless blessed $exch_frame and $exch_frame->isa('Net::AMQP::Protocol::Exchange::DeclareOk');
        my $queue_frame = $self->_declare_queue(
            name => $name,
            durable => 1,
        )->method_frame;
        my $bind_frame = $self->_bind_queue(
            queue => $queue_frame->queue,
            exchange => $name,
            routing_key => '#',
        )->method_frame;
        die Dumper($bind_frame) unless blessed $bind_frame and $bind_frame->isa('Net::AMQP::Protocol::Queue::BindOk');
    }
}

method BUILD {
    state51::Base->connect;
}

method run {
    $self->load_all_jobs;
    $self->bind_all_queues;
    my $done = AnyEvent->condvar;
    $self->_consume(
        on_consume => sub {
            my $message = shift;
            print $message->{deliver}->method_frame->routing_key,
                ': ', $message->{body}->payload, "\n";
            # FIXME - deal with not being able to unserialize
            my $data = decode_json($message->{body}->payload);
            my $class = $data->{__CLASS__}; # FIXME - Deal with bad class.
            fork_call {
                state51::Base->unhook_storage_db;
                state51::Base->connect;
                my $job = $class->unpack($data);
                my $ret = $job->run;
                state51::Base->unhook_storage_db;
                return $ret;
            } sub {
                if (scalar @_) {
                    warn("Job ran, returned " . shift);
                }
                else {
                    warn("Job failed, returned " . $@);
                }
            }
        },
    );
    $done->recv; # Go into the event loop forever.
}

with qw/
    state51::Script
/;

__PACKAGE__->meta->make_immutable;
__PACKAGE__->new_with_options->run unless caller;
1;

1;
