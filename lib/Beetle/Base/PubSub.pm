package Beetle::Base::PubSub;

use Moose;
extends qw(Beetle::Base);

# Base class for publisher/subscriber

my @QUEUE_CREATION_KEYS = qw(passive durable exclusive auto_delete no_wait);
my @QUEUE_BINDING_KEYS  = qw(key no_wait);

has 'client' => (
    is  => 'ro',
    isa => 'Any',
);

# def exchanges
#   @exchanges[@server] ||= {}
# end
has 'exchanges' => (
    default => sub {
        { shift->server => {} }
    },
    handles => {
        get_exchange => 'get',
        has_exchange => 'exists',
        set_exchange => 'set',
    },
    is     => 'ro',
    isa    => 'HashRef',
    lazy   => 1,
    traits => [qw(Hash)],
);

has 'options' => (
    is  => 'ro',
    isa => 'Any',
);

# def queues
#   @queues[@server] ||= {}
# end
has 'queues' => (
    default => sub {
        { shift->server => {} }
    },
    is   => 'ro',
    isa  => 'HashRef',
    lazy => 1,
);

# def set_current_server(s)
#   @server = s
# end
has 'server' => (
    is     => 'ro',
    isa    => 'Any',
    writer => 'set_current_server',
);

has 'servers' => (
    default => sub { [] },
    handles => {
        all_servers => 'elements',
        get_server  => 'get',
    },
    is     => 'ro',
    isa    => 'ArrayRef',
    traits => [qw(Array)],
);

sub BUILD {
    my ($self) = @_;
    my $servers = $self->client->servers;
    $self->{servers} = $servers;
    $self->{server} = $servers->[ int rand scalar @$servers ];
}

sub error {
    my ( $self, $message ) = @_;
    $self->log->error($message);
    die $message;
}

# def current_host
#   @server.split(':').first
# end
sub current_host {
    my ($self) = @_;
    return ( split /:/, $self->server )[0];
}

# def current_port
#   @server =~ /:(\d+)$/ ? $1.to_i : 5672
# end
sub current_port {
    my ($self) = @_;
    return ( split /:/, $self->server )[1] || 5672;
}

# def each_server
#   @servers.each { |s| set_current_server(s); yield }
# end
sub each_server {
    my ( $self, $code ) = @_;

    # TODO: <plu> not sure I got 'yield' right here
    foreach my $server ( $self->all_servers ) {
        $self->set_current_server($server);
        $code->();
    }
}

# def exchange(name)
#   exchanges[name] ||= create_exchange!(name, @client.exchanges[name])
# end
sub exchange {
    my ( $self, $name ) = @_;
    unless ( $self->has_exchange($name) ) {
        my $exchange = $self->create_exchange( $name => $self->client->get_exchange($name) );
        $self->set_exchange( $name => $exchange );
    }
}

# def queue(name)
#   queues[name] ||=
#     begin
#       opts = @client.queues[name]
#       error("You are trying to bind a queue #{name} which is not configured!") unless opts
#       logger.debug("Beetle: binding queue #{name} with internal name #{opts[:amqp_name]} on server #{@server}")
#       queue_name = opts[:amqp_name]
#       creation_options = opts.slice(*QUEUE_CREATION_KEYS)
#       the_queue = nil
#       @client.bindings[name].each do |binding_options|
#         exchange_name = binding_options[:exchange]
#         binding_options = binding_options.slice(*QUEUE_BINDING_KEYS)
#         the_queue = bind_queue!(queue_name, creation_options, exchange_name, binding_options)
#       end
#       the_queue
#     end
# end
sub queue {
    my ( $self, $name ) = @_;

    my $options = $self->client->get_queue($name);
    $self->error("You are trying to bind a queue ${name} which is not configured!") unless $options;

    $self->logger->debug( sprintf 'Beetle: binding queue %s with internal name %s on server %s',
        $name, $options->{amqp_name}, $self->server );

    my $queue_name = $options->{amqp_name};
    my $the_queue;

    my $creation_options = {};
    foreach my $key (@QUEUE_CREATION_KEYS) {
        $creation_options->{$key} = $options->{$key} if exists $options->{$key};
    }

    my $bindings = $self->client->get_binding($name);
    foreach my $binding_options (@$bindings) {
        my $exchange_name = $binding_options->{exchange};
        foreach my $key ( keys %$binding_options ) {
            delete $binding_options->{$key} unless grep $_ eq $key, @QUEUE_BINDING_KEYS;
        }
        $the_queue = $self->bind_queue( $queue_name, $creation_options, $exchange_name, $binding_options );
    }

    return $the_queue;
}

1;
