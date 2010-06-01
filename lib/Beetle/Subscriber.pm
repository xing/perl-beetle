package Beetle::Subscriber;

use Moose;
use Hash::Merge::Simple qw( merge );
use Beetle::Handler;
use Beetle::Message;
extends qw(Beetle::Base::PubSub);

has 'handlers' => (
    default => sub { {} },
    handles => {
        get_handler => 'get',
        has_handler => 'exists',
        set_handler => 'set',
    },
    is     => 'ro',
    isa    => 'HashRef',
    traits => [qw(Hash)],
);

has 'amqp_connections' => (
    default => sub { {} },
    is      => 'ro',
    isa     => 'HashRef',
    traits  => [qw(Hash)],
);

has 'mqs' => (
    default => sub { {} },
    handles => {
        get_mq => 'get',
        has_mq => 'exists',
        set_mq => 'set',
    },
    is     => 'ro',
    isa    => 'HashRef',
    traits => [qw(Hash)],
);

# # the client calls this method to subcsribe to all queues on all servers which have
# # handlers registered for the given list of messages. this method does the following
# # things:
# #
# # * creates all exchanges which have been registered for the given messages
# # * creates and binds queues which have been registered for the exchanges
# # * subscribes the handlers for all these queues
# #
# # yields before entering the eventmachine loop (if a block was given)
# def listen(messages) #:nodoc:
#   EM.run do
#     exchanges = exchanges_for_messages(messages)
#     create_exchanges(exchanges)
#     queues = queues_for_exchanges(exchanges)
#     bind_queues(queues)
#     subscribe_queues(queues)
#     yield if block_given?
#   end
# end
sub listen {
    my ( $self, $messages, $code ) = @_;
    my $exchanges = $self->exchanges_for_messages($messages);
    $self->create_exchanges($exchanges);
    my $queues = $self->queues_for_exchanges($exchanges);
    $self->bind_queues($queues);
    $self->subscribe_queues($queues);
    $code->() if defined $code && ref $code eq 'CODE';
    $self->bunny->listen;
}

# # stops the eventmachine loop
# def stop! #:nodoc:
#   EM.stop_event_loop
# end
sub stop {
    my ($self) = @_;
    $self->bunny->stop;
}

# # register handler for the given queues (see Client#register_handler)
# def register_handler(queues, opts={}, handler=nil, &block) #:nodoc:
#   Array(queues).each do |queue|
#     @handlers[queue] = [opts.symbolize_keys, handler || block]
#   end
# end
sub register_handler {
    my ( $self, $queues, $options, $handler ) = @_;
    foreach my $queue (@$queues) {
        $self->set_handler(
            $queue => {
                code    => $handler,
                options => $options,
            }
        );
    }
}

# def exchanges_for_messages(messages)
#   @client.messages.slice(*messages).map{|_, opts| opts[:exchange]}.uniq
# end
sub exchanges_for_messages {
    my ( $self, $messages ) = @_;
    my %exchanges = ();
    foreach my $m (@$messages) {
        my $message = $self->client->get_message($m);
        next unless $message;
        my $exchange = $message->{exchange};
        $exchanges{$exchange} = 1;
    }
    return [ keys %exchanges ];
}

# def queues_for_exchanges(exchanges)
#   @client.exchanges.slice(*exchanges).map{|_, opts| opts[:queues]}.flatten.uniq
# end
sub queues_for_exchanges {
    my ( $self, $exchanges ) = @_;
    my %queues = ();
    foreach my $e (@$exchanges) {
        my $exchange = $self->client->get_exchange($e);
        next unless $exchange;
        my $q = $exchange->{queues};
        $queues{$_} = 1 for @$q;
    }
    return [ keys %queues ];
}

# def create_exchanges(exchanges)
#   each_server do
#     exchanges.each { |name| exchange(name) }
#   end
# end
sub create_exchanges {
    my ( $self, $exchanges ) = @_;
    $self->each_server(
        sub {
            my $self = shift;
            foreach my $exchange (@$exchanges) {
                $self->exchange($exchange);
            }
        }
    );
}

# def bind_queues(queues)
#   each_server do
#     queues.each { |name| queue(name) }
#   end
# end
sub bind_queues {
    my ( $self, $queues ) = @_;
    $self->each_server(
        sub {
            my $self = shift;
            foreach my $queue (@$queues) {
                $self->queue($queue);
            }
        }
    );
}

# def subscribe_queues(queues)
#   each_server do
#     queues.each { |name| subscribe(name) if @handlers.include?(name) }
#   end
# end
sub subscribe_queues {
    my ( $self, $queues ) = @_;
    $self->each_server(
        sub {
            my $self = shift;
            foreach my $queue (@$queues) {
                $self->subscribe($queue) if $self->has_handler($queue);
            }
        }
    );
}

# # returns the mq object for the given server or returns a new one created with the
# # prefetch(1) option. this tells it to just send one message to the receiving buffer
# # (instead of filling it). this is necesssary to ensure that one subscriber always just
# # handles one single message. we cannot ensure reliability if the buffer is filled with
# # messages and crashes.
# def mq(server=@server)
#   @mqs[server] ||= MQ.new(amqp_connection).prefetch(1)
# end
sub mq {
    my ( $self, $server ) = @_;

    # TODO: <plu> make sure that buffer things is possible in Perl's rabbitmq lib as well
    $server ||= $self->server;
    $self->set_mq( $server => 'TODO: Add MQ object - wtf' ) unless $self->has_mq($server);
    return 'MQ-Object';
}

# def subscribe(queue_name)
#   error("no handler for queue #{queue_name}") unless @handlers.include?(queue_name)
#   opts, handler = @handlers[queue_name]
#   queue_opts = @client.queues[queue_name][:amqp_name]
#   amqp_queue_name = queue_opts
#   callback = create_subscription_callback(queue_name, amqp_queue_name, handler, opts)
#   logger.debug "Beetle: subscribing to queue #{amqp_queue_name} with key # on server #{@server}"
#   begin
#     queues[queue_name].subscribe(opts.slice(*SUBSCRIPTION_KEYS).merge(:key => "#", :ack => true), &callback)
#   rescue MQ::Error
#     error("Beetle: binding multiple handlers for the same queue isn't possible.")
#   end
# end
sub subscribe {
    my ( $self, $queue_name ) = @_;

    $self->error( sprintf 'no handler for queue %s', $queue_name ) unless $self->has_handler($queue_name);

    my $handler         = $self->get_handler($queue_name);
    my $amqp_queue_name = $self->client->get_queue($queue_name)->{amqp_name};

    my $callback =
      $self->create_subscription_callback( $queue_name, $amqp_queue_name, $handler->{code}, $handler->{options} );

    $self->log->debug( sprintf 'Beetle: subscribing to queue %s with key # on server %s',
        $amqp_queue_name, $self->server );

    eval {
        $self->bunny->subscribe( $queue_name => $callback );    # TODO: <plu> implement this.
    };
    if ($@) {
        $self->error('Beetle: binding multiple handlers for the same queue isn\'t possible');
    }
}

# def create_subscription_callback(queue_name, amqp_queue_name, handler, opts)
#   server = @server
#   lambda do |header, data|
#     begin
#       processor = Handler.create(handler, opts)
#       message_options = opts.merge(:server => server, :store => @client.deduplication_store)
#       m = Message.new(amqp_queue_name, header, data, message_options)
#       result = m.process(processor)
#       if result.recover?
#         sleep 1
#         mq(server).recover
#       elsif reply_to = header.properties[:reply_to]
#         status = result == Beetle::RC::OK ? "OK" : "FAILED"
#         exchange = MQ::Exchange.new(mq(server), :direct, "", :key => reply_to)
#         exchange.publish(m.handler_result.to_s, :headers => {:status => status})
#       end
#     rescue Exception
#       Beetle::reraise_expectation_errors!
#       # swallow all exceptions
#       logger.error "Beetle: internal error during message processing: #{$!}: #{$!.backtrace.join("\n")}"
#     end
#   end
# end
sub create_subscription_callback {
    my ( $self, $queue_name, $amqp_queue_name, $handler, $options ) = @_;
    return sub {
        my ($amqp_message) = @_;
        my $header         = $amqp_message->{header};
        my $body           = $amqp_message->{body}->payload;
        eval {
            my $processor = Beetle::Handler->create( $handler, $options );
            my $message_options = merge $options,
              { server => $self->server, store => $self->client->deduplication_store };
            my $message = Beetle::Message->new(
                queue  => $amqp_queue_name,
                header => $header,
                body   => $body,
                %$message_options,
            );
            my $result = $message->process($processor);

            # TODO: complete the implementation
            return $result;
        };
        if ($@) {
            warn $@;

            # TODO: <plu> add exception handling
        }
    };
}

# def bind_queue!(queue_name, creation_keys, exchange_name, binding_keys)
#   queue = mq.queue(queue_name, creation_keys)
#   exchange = exchange(exchange_name)
#   queue.bind(exchange, binding_keys)
#   queue
# end
sub bind_queue {
    my ( $self, $queue_name, $creation_keys, $exchange_name, $binding_keys ) = @_;
    $self->bunny->queue_declare( $queue_name => $creation_keys );
    $self->exchange($exchange_name);
    $self->bunny->queue_bind( $queue_name, $exchange_name, $binding_keys->{key} );
}

# def amqp_connection(server=@server)
#   @amqp_connections[server] ||= new_amqp_connection
# end
#
# def new_amqp_connection
#   # FIXME: wtf, how to test that reconnection feature....
#   con = AMQP.connect(:host => current_host, :port => current_port,
#                      :user => Beetle.config.user, :pass => Beetle.config.password, :vhost => Beetle.config.vhost)
#   con.instance_variable_set("@on_disconnect", proc{ con.__send__(:reconnect) })
#   con
# end

1;
