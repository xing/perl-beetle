package Beetle::Publisher;

use Moose;
use Hash::Merge::Simple qw( merge );
use Net::RabbitMQ;
use Beetle::Message;
use Data::Dumper;
extends qw(Beetle::Base::PubSub);

my $RPC_DEFAULT_TIMEOUT = 10;

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
        remove_dead_server => 'delete',
        set_dead_server    => 'set',
    },
    is        => 'ro',
    isa       => 'HashRef',
    predicate => 'has_dead_servers',
    traits    => [qw(Hash)],
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

# def publish(message_name, data, opts={}) #:nodoc:
#   opts = @client.messages[message_name].merge(opts.symbolize_keys)
#   exchange_name = opts.delete(:exchange)
#   opts.delete(:queue)
#   recycle_dead_servers unless @dead_servers.empty?
#   if opts[:redundant]
#     publish_with_redundancy(exchange_name, message_name, data, opts)
#   else
#     publish_with_failover(exchange_name, message_name, data, opts)
#   end
# end
#
sub publish {
    my ( $self, $message_name, $data, $options ) = @_;
    $options ||= {};

    my $message = $self->client->get_message($message_name);
    $options = merge $options, $message;

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

# def publish_with_failover(exchange_name, message_name, data, opts) #:nodoc:
#   tries = @servers.size
#   logger.debug "Beetle: sending #{message_name}"
#   published = 0
#   opts = Message.publishing_options(opts)
#   begin
#     select_next_server
#     bind_queues_for_exchange(exchange_name)
#     logger.debug "Beetle: trying to send message #{message_name}:#{opts[:message_id]} to #{@server}"
#     exchange(exchange_name).publish(data, opts)
#     logger.debug "Beetle: message sent!"
#     published = 1
#   rescue Bunny::ServerDownError, Bunny::ConnectionError
#     stop!
#     mark_server_dead
#     tries -= 1
#     retry if tries > 0
#     logger.error "Beetle: message could not be delivered: #{message_name}"
#   end
#   published
# end
sub publish_with_failover {
    my ( $self, $exchange_name, $message_name, $data, $options ) = @_;

    $self->log->debug( sprintf 'Beetle: sending %s', $message_name );

    my $tries     = $self->count_servers;
    my $published = 0;

    $options = Beetle::Message->publishing_options(%$options);

    for ( 1 .. $tries ) {
        $self->select_next_server;
        $self->bind_queues_for_exchange($exchange_name);

        $self->log->debug(
            sprintf 'Beetle: trying to send message %s:%s to %s',
            $message_name, $options->{message_id},
            $self->server
        );

        eval {
            my $exchange = $self->exchange($exchange_name);
            use Data::Dumper;
            $Data::Dumper::Sortkeys=1;
            warn Dumper $exchange;
            $self->bunny->publish( 1, $message_name, $data, { exchange => $exchange_name } );
        };
        unless ($@) {
            $published = 1;
            $self->log->debug('Beetle: message sent!');
            last;
        }
        else {
            $self->log->error($@);
        }

        $self->stop;
        $self->mark_server_dead;
        $self->log->error( sprintf 'Beetle: message could not be delivered: %s', $message_name );
    }

    return $published;
}

# def publish_with_redundancy(exchange_name, message_name, data, opts) #:nodoc:
#   if @servers.size < 2
#     logger.error "Beetle: at least two active servers are required for redundant publishing"
#     return publish_with_failover(exchange_name, message_name, data, opts)
#   end
#   published = []
#   opts = Message.publishing_options(opts)
#   loop do
#     break if published.size == 2 || @servers.empty? || published == @servers
#     begin
#       select_next_server
#       next if published.include? @server
#       bind_queues_for_exchange(exchange_name)
#       logger.debug "Beetle: trying to send #{message_name}:#{opts[:message_id]} to #{@server}"
#       exchange(exchange_name).publish(data, opts)
#       published << @server
#       logger.debug "Beetle: message sent (#{published})!"
#     rescue Bunny::ServerDownError, Bunny::ConnectionError
#       stop!
#       mark_server_dead
#     end
#   end
#   case published.size
#   when 0
#     logger.error "Beetle: message could not be delivered: #{message_name}"
#   when 1
#     logger.warn "Beetle: failed to send message redundantly"
#   end
#   published.size
# end
sub publish_with_redundancy {
    my ( $self, $exchange_name, $message_name, $data, $options ) = @_;

    if ( $self->count_servers < 2 ) {
        $self->log->error('Beetle: at least two active servers are required for redundant publishing');
        return $self->publish_with_failover( $exchange_name, $message_name, $data, $options );
    }

    my @published = ();

    $options = Beetle::Message->publishing_options(%$options);

    while (1) {
        last if scalar(@published) == 2;
        last unless $self->count_servers;
        last if scalar(@published) == $self->count_servers;

        $self->select_next_server;
        next if grep $_ eq $self->server, @published;

        $self->bind_queues_for_exchange($exchange_name);

        $self->log->debug(
            sprintf 'Beetle: trying to send message %s:%s to %s',
            $message_name, $options->{message_id},
            $self->server
        );

        eval { $self->exchange($exchange_name)->publish( $data, $options ); };
        unless ($@) {
            push @published, $self->server;
            $self->log->debug( sprintf 'Beetle: message sent ()!', scalar(@published) );
        }

        $self->stop;
        $self->mark_server_dead;
    }

    if ( scalar(@published) == 0 ) {
        $self->log->error( sprintf 'Beetle: message could not be delivered: %s', $message_name );
    }
    elsif ( scalar(@published) == 1 ) {
        $self->log->error('Beetle: failed to send message redundantly');
    }

    return scalar @published;
}

# def rpc(message_name, data, opts={}) #:nodoc:
#   opts = @client.messages[message_name].merge(opts.symbolize_keys)
#   exchange_name = opts.delete(:exchange)
#   opts.delete(:queue)
#   recycle_dead_servers unless @dead_servers.empty?
#   tries = @servers.size
#   logger.debug "Beetle: performing rpc with message #{message_name}"
#   result = nil
#   status = "TIMEOUT"
#   begin
#     select_next_server
#     bind_queues_for_exchange(exchange_name)
#     # create non durable, autodeleted temporary queue with a server assigned name
#     queue = bunny.queue
#     opts = Message.publishing_options(opts.merge :reply_to => queue.name)
#     logger.debug "Beetle: trying to send #{message_name}:#{opts[:message_id]} to #{@server}"
#     exchange(exchange_name).publish(data, opts)
#     logger.debug "Beetle: message sent!"
#     logger.debug "Beetle: listening on reply queue #{queue.name}"
#     queue.subscribe(:message_max => 1, :timeout => opts[:timeout] || RPC_DEFAULT_TIMEOUT) do |msg|
#       logger.debug "Beetle: received reply!"
#       result = msg[:payload]
#       status = msg[:header].properties[:headers][:status]
#     end
#     logger.debug "Beetle: rpc complete!"
#   rescue Bunny::ServerDownError, Bunny::ConnectionError
#     stop!
#     mark_server_dead
#     tries -= 1
#     retry if tries > 0
#     logger.error "Beetle: message could not be delivered: #{message_name}"
#   end
#   [status, result]
# end
sub rpc {
    my ( $self, $message_name, $data, $options ) = @_;
    $options ||= {};

    my $exchange_name = delete $options->{exchange};
    delete $options->{queue};

    $self->recycle_dead_servers if $self->has_dead_servers;

    $self->log->debug( sprintf 'Beetle: performing rpc with message %s', $message_name );

    my $status = 'TIMEOUT';
    my $tries  = $self->count_servers;
    my $result;

    $self->select_next_server;
    $self->bind_queues_for_exchange($exchange_name);

    # TODO: <plu> Check rabbitmq-interface if this is correct

    # create non durable, autodeleted temporary queue with a server assigned name
    my $queue = $self->bunny->queue;
    $options = Beetle::Message->publishing_options( %$options, reply_to => $queue->{name} );

    $self->log->debug(
        sprintf 'Beetle: trying to send message %s:%s to %s',
        $message_name, $options->{message_id},
        $self->server
    );

    eval { $self->exchange($exchange_name)->publish( $data, $options ); };
    unless ($@) {
        $self->log->debug('Beetle: message sent!');
        $self->log->debug( sprintf 'Beetle: listening on reply queue %s', $queue->{name} );

        # TODO: <plu> finish this
        # $queue->subscribe( message_max => 1, timeout => $options->{timeout} || $RPC_DEFAULT_TIMEOUT );
        #     queue.subscribe(:message_max => 1, :timeout => opts[:timeout] || RPC_DEFAULT_TIMEOUT) do |msg|
        #       logger.debug "Beetle: received reply!"
        #       result = msg[:payload]
        #       status = msg[:header].properties[:headers][:status]
        #     end
        #     logger.debug "Beetle: rpc complete!"
    }

    return ( $status, $result );
}

# def purge(queue_name) #:nodoc:
#   each_server { queue(queue_name).purge rescue nil }
# end
sub purge {
    my ( $self, $queue_name ) = @_;
    $self->each_server(
        sub {
            my $self = shift;

            # TODO: <plu> finish this
        }
    );
}

# def stop #:nodoc:
#   each_server { stop! }
# end
# def stop!
#   begin
#     bunny.stop
#   rescue Exception
#     Beetle::reraise_expectation_errors!
#   ensure
#     @bunnies[@server] = nil
#     @exchanges[@server] = {}
#     @queues[@server] = {}
#   end
# end
sub stop {
    my ($self) = @_;
    $self->each_server(
        sub {
            my $self = shift;

            # TODO: <plu> proper exception handling missing
            eval { $self->bunny->disconnect };

            $self->{bunnies}{ $self->server }   = undef;
            $self->{exchanges}{ $self->server } = {};
            $self->{queues}{ $self->server }    = {};
        }
    );
}

# private

# def bunny
#   @bunnies[@server] ||= new_bunny
# end
sub bunny {
    my ($self) = @_;
    $self->set_bunny( $self->server => $self->new_bunny ) unless $self->has_bunny( $self->server );
    return $self->get_bunny( $self->server );
}

# def new_bunny
#   b = Bunny.new(:host => current_host, :port => current_port, :logging => !!@options[:logging],
#                 :user => Beetle.config.user, :pass => Beetle.config.password, :vhost => Beetle.config.vhost)
#   b.start
#   b
# end
sub new_bunny {
    my ($self) = @_;
    my $b = Net::RabbitMQ->new;

    # TODO: <plu> not sure if it's a good idea to connect here
    $b->connect(
        $self->current_host => {
            user     => $self->config->user,
            password => $self->config->password,
            vhost    => $self->config->vhost,
            port     => $self->current_port,
        }
    );

    # TODO: <plu> mmm... which channel?!
    $b->channel_open(1);

    return $b;
}

# def recycle_dead_servers
#   recycle = []
#   @dead_servers.each do |s, dead_since|
#     recycle << s if dead_since < 10.seconds.ago
#   end
#   @servers.concat recycle
#   recycle.each {|s| @dead_servers.delete(s)}
# end
sub recycle_dead_servers {
    my ($self)  = @_;
    my @recycle = ();
    my %servers = $self->all_dead_servers;
    while ( my ( $server, $time ) = each %servers ) {
        if ( time - $time < 10 ) {
            push @recycle, $server;
            $self->remove_dead_server($server);
        }
    }
    $self->add_server(@recycle);
}

# def mark_server_dead
#   logger.info "Beetle: server #{@server} down: #{$!}"
#   @dead_servers[@server] = Time.now
#   @servers.delete @server
#   @server = @servers[rand @servers.size]
# end
sub mark_server_dead {
    my ($self) = @_;

    # TODO: <plu> no clue how to get the error message here
    $self->log->info( sprintf 'Beetle: server %s down: %s', $self->server, 'TODO' );

    $self->set_dead_server( $self->server => time );

    my @servers = grep $_ ne $self->server, $self->all_servers;
    $self->{servers} = \@servers;
    $self->{server}  = $servers[ int rand scalar @servers ];
}

# def select_next_server
#   return logger.error("Beetle: message could not be delivered - no server available") && 0 if @servers.empty?
#   set_current_server(@servers[((@servers.index(@server) || 0)+1) % @servers.size])
# end
sub select_next_server {
    my ($self) = @_;
    unless ( $self->count_servers ) {
        $self->log->error('Beetle: message could not be delivered - no server available');
        return 0;
    }
    my $index = 0;
    foreach my $server ( $self->all_servers ) {
        last if $server eq $self->server;
        $index++;
    }
    my $next = ( $index + 1 ) % $self->count_servers;
    $self->set_current_server( $self->get_server($next) );
}

# def create_exchange!(name, opts)
#   bunny.exchange(name, opts)
# end
sub create_exchange {
    my ( $self, $name, $options ) = @_;
    my %rmq_options = %{ $options || {} };
    $rmq_options{exchange_type} = delete $rmq_options{type};
    delete $rmq_options{queues};
    $self->bunny->exchange_declare( 1, $name => \%rmq_options );
    return;
}

# def bind_queues_for_exchange(exchange_name)
#   return if @exchanges_with_bound_queues.include?(exchange_name)
#   @client.exchanges[exchange_name][:queues].each {|q| queue(q) }
#   @exchanges_with_bound_queues[exchange_name] = true
# end
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

# # TODO: Refactor, fethch the keys and stuff itself
# def bind_queue!(queue_name, creation_keys, exchange_name, binding_keys)
#   logger.debug("Creating queue with opts: #{creation_keys.inspect}")
#   queue = bunny.queue(queue_name, creation_keys)
#   logger.debug("Binding queue #{queue_name} to #{exchange_name} with opts: #{binding_keys.inspect}")
#   queue.bind(exchange(exchange_name), binding_keys)
#   queue
# end
sub bind_queue {
    my ( $self, $queue_name, $creation_keys, $exchange_name, $binding_keys ) = @_;
    $self->log->debug( sprintf 'Creating queue with options: %s', Dumper($creation_keys) );
    $self->bunny->queue_declare( 1, $queue_name, $creation_keys );
    $self->log->debug( sprintf 'Binding queue %s to %s with options %s',
        $queue_name, $exchange_name, Dumper($binding_keys) );
    $self->exchange($exchange_name);
    $self->bunny->queue_bind( 1, $queue_name, $exchange_name, $binding_keys->{key} );
}

1;
