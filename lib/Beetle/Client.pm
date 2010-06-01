package Beetle::Client;

# This class provides the interface through which messaging is configured for both
# message producers and consumers. It keeps references to an instance of a
# Beetle::Subscriber, a Beetle::Publisher (both of which are instantiated on demand),
# and a reference to an instance of Beetle::DeduplicationStore.
#
# Configuration of exchanges, queues, messages, and message handlers is done by calls to
# corresponding register_ methods. Note that these methods just build up the
# configuration, they don't interact with the AMQP servers.
#
# On the publisher side, publishing a message will ensure that the exchange it will be
# sent to, and each of the queues bound to the exchange, will be created on demand. On
# the subscriber side, exchanges, queues, bindings and queue subscriptions will be
# created when the application calls the listen method. An application can decide to
# subscribe to only a subset of the configured queues by passing a list of queue names
# to the listen method.
#
# The net effect of this strategy is that producers and consumers can be started in any
# order, so that no message is lost if message producers are accidentally started before
# the corresponding consumers.

use Moose;
use Beetle::Config;
use Beetle::DeduplicationStore;
use Beetle::Publisher;
use Beetle::Subscriber;
use Sys::Hostname;

has 'config' => (
    default => sub { Beetle::Config->new },
    is      => 'ro',
    isa     => 'Beetle::Config',
);

has 'servers' => (
    documentation => 'the AMQP servers available for publishing',
    is            => 'ro',
    isa           => 'ArrayRef',
);

has 'exchanges' => (
    default       => sub { {} },
    documentation => 'an options hash for the configured exchanges',
    handles       => {
        get_exchange => 'get',
        has_exchange => 'exists',
        set_exchange => 'set',
    },
    is     => 'rw',
    isa    => 'HashRef',
    traits => [qw(Hash)],
);

has 'queues' => (
    default       => sub { {} },
    documentation => 'an options hash for the configured queues',
    handles       => {
        all_queues => 'elements',
        get_queue  => 'get',
        has_queue  => 'exists',
        set_queue  => 'set',
    },
    is     => 'rw',
    isa    => 'HashRef',
    traits => [qw(Hash)],
);

has 'bindings' => (
    default       => sub { {} },
    documentation => 'an options hash for the configured queue bindings',
    handles       => {
        get_binding => 'get',
        has_binding => 'exists',
        set_binding => 'set',
    },
    is     => 'rw',
    isa    => 'HashRef',
    traits => [qw(Hash)],
);

has 'messages' => (
    default       => sub { {} },
    documentation => 'an options hash for the configured messages',
    handles       => {
        get_message   => 'get',
        has_message   => 'exists',
        message_names => 'keys',
        set_message   => 'set',
    },
    is     => 'rw',
    isa    => 'HashRef',
    traits => [qw(Hash)],
);

has 'deduplication_store' => (
    documentation => 'the deduplication store to use for this client',
    is            => 'ro',
    isa           => 'Beetle::DeduplicationStore',
);

has 'publisher' => (
    default => sub { Beetle::Publisher->new( client => shift ) },
    is      => 'ro',
    isa     => 'Beetle::Publisher',
    lazy    => 1,
);

has 'subscriber' => (
    default => sub { Beetle::Subscriber->new( client => shift ) },
    is      => 'ro',
    isa     => 'Beetle::Subscriber',
    lazy    => 1,
);

sub BUILD {
    my ($self) = @_;
    $self->{deduplication_store} = Beetle::DeduplicationStore->new(
        hosts => $self->config->redis_hosts,
        db    => $self->config->redis_db,
    );
    $self->{servers} = [ split / *, */, $self->config->servers ];
}

# register an exchange with the given _name_ and a set of _options_:
# [<tt>:type</tt>]
#   the type option will be overwritten and always be <tt>:topic</tt>, beetle does not allow fanout exchanges
# [<tt>:durable</tt>]
#   the durable option will be overwritten and always be true. this is done to ensure that exchanges are never deleted
#
# def register_exchange(name, options={})
#   name = name.to_s
#   raise ConfigurationError.new("exchange #{name} already configured") if exchanges.include?(name)
#   exchanges[name] = options.symbolize_keys.merge(:type => :topic, :durable => true)
# end

sub register_exchange {
    my ( $self, $name, $options ) = @_;
    $options ||= {};

    die "exchange ${name} already configured" if $self->has_exchange($name);

    $options->{durable} = 1;
    $options->{type}    = 'topic';

    $self->set_exchange( $name => $options );
}

# register a durable, non passive, non auto_deleted queue with the given _name_ and an _options_ hash:
# [<tt>:exchange</tt>]
#   the name of the exchange this queue will be bound to (defaults to the name of the queue)
# [<tt>:key</tt>]
#   the binding key (defaults to the name of the queue)
# automatically registers the specified exchange if it hasn't been registered yet

# def register_queue(name, options={})
#   name = name.to_s
#   raise ConfigurationError.new("queue #{name} already configured") if queues.include?(name)
#   opts = {:exchange => name, :key => name}.merge!(options.symbolize_keys)
#   opts.merge! :durable => true, :passive => false, :exclusive => false, :auto_delete => false, :amqp_name => name
#   exchange = opts.delete(:exchange).to_s
#   key = opts.delete(:key)
#   queues[name] = opts
#   register_binding(name, :exchange => exchange, :key => key)
# end

sub register_queue {
    my ( $self, $name, $options ) = @_;
    $options ||= {};

    die "queue ${name} already configured" if $self->has_queue($name);

    # TODO: <plu> not sure if i got this opts.merge! right here...
    $options->{exchange} ||= $name;
    $options->{key}      ||= $name;
    $options->{durable}     = 1;
    $options->{passive}     = 0;
    $options->{exclusive}   = 0;
    $options->{auto_delete} = 0;
    $options->{amqp_name}   = $name;

    my $exchange = delete $options->{exchange};
    my $key      = delete $options->{key};

    $self->set_queue( $name => $options );
    $self->register_binding( $name => { exchange => $exchange, key => $key } );
}

# register an additional binding for an already configured queue _name_ and an _options_ hash:
# [<tt>:exchange</tt>]
#   the name of the exchange this queue will be bound to (defaults to the name of the queue)
# [<tt>:key</tt>]
#   the binding key (defaults to the name of the queue)
# automatically registers the specified exchange if it hasn't been registered yet

# def register_binding(queue_name, options={})
#   name = queue_name.to_s
#   opts = options.symbolize_keys
#   exchange = (opts[:exchange] || name).to_s
#   key = (opts[:key] || name).to_s
#   (bindings[name] ||= []) << {:exchange => exchange, :key => key}
#   register_exchange(exchange) unless exchanges.include?(exchange)
#   queues = (exchanges[exchange][:queues] ||= [])
#   queues << name unless queues.include?(name)
# end

sub register_binding {
    my ( $self, $queue_name, $options ) = @_;
    $options ||= {};

    my $exchange = $options->{exchange} || $queue_name;
    my $key      = $options->{key}      || $queue_name;

    $self->add_binding( $queue_name => { exchange => $exchange, key => $key } );
    $self->register_exchange($exchange) unless $self->has_exchange($exchange);

    my $queues = $self->get_exchange($exchange)->{queues} || [];
    push @$queues, $queue_name unless grep $_ eq $queue_name, @$queues;
    $self->get_exchange($exchange)->{queues} = $queues;

    # TODO: <plu> not sure if I got this right.
}

sub add_binding {
    my ( $self, $queue_name, $item ) = @_;
    $self->set_binding( $queue_name => [] ) unless $self->has_binding($queue_name);
    my $binding = $self->get_binding($queue_name);
    push @$binding, $item;
    $self->set_binding( $queue_name => $binding );
}

# register a persistent message with a given _name_ and an _options_ hash:
# [<tt>:key</tt>]
#   specifies the routing key for message publishing (defaults to the name of the message)
# [<tt>:ttl</tt>]
#   specifies the time interval after which the message will be silently dropped (seconds).
#   defaults to Message::DEFAULT_TTL.
# [<tt>:redundant</tt>]
#   specifies whether the message should be published redundantly (defaults to false)

# def register_message(message_name, options={})
#   name = message_name.to_s
#   raise ConfigurationError.new("message #{name} already configured") if messages.include?(name)
#   opts = {:exchange => name, :key => name}.merge!(options.symbolize_keys)
#   opts.merge! :persistent => true
#   opts[:exchange] = opts[:exchange].to_s
#   messages[name] = opts
# end

sub register_message {
    my ( $self, $message_name, $options ) = @_;
    $options ||= {};

    die "message ${message_name} already configured" if $self->has_message($message_name);
    $options->{exchange} ||= $message_name;
    $options->{key}      ||= $message_name;
    $options->{persistent} = 1;

    $self->set_message( $message_name => $options );
}

# registers a handler for a list of queues (which must have been registered
# previously). The handler will be invoked when any messages arrive on the queue.
#
# Examples:
#   register_handler([:foo, :bar], :timeout => 10.seconds) { |message| puts "received #{message}" }
#
#   on_error   = lambda{ puts "something went wrong with baz" }
#   on_failure = lambda{ puts "baz has finally failed" }
#
#   register_handler(:baz, :exceptions => 1, :errback => on_error, :failback => on_failure) { puts "received baz" }
#
#   register_handler(:bar, BarHandler)
#
# For details on handler classes see class Beetle::Handler

# def register_handler(queues, *args, &block)
#   queues = Array(queues).map(&:to_s)
#   queues.each {|q| raise UnknownQueue.new(q) unless self.queues.include?(q)}
#   opts = args.last.is_a?(Hash) ? args.pop : {}
#   handler = args.shift
#   raise ArgumentError.new("too many arguments for handler registration") unless args.empty?
#   subscriber.register_handler(queues, opts, handler, &block)
# end

# bah, this is odd, we won't do it the ruby way having a dynamic length of arguments "in the middle"
sub register_handler {
    my ( $self, $queues, $handler, $handler_args ) = @_;
    $handler_args ||= {};
    $queues = [$queues] unless ref $queues eq 'ARRAY';

    foreach my $queue (@$queues) {
        die "unknown queue: $queue" unless $self->has_queue($queue);    # TODO: <plu> add proper exception handling
    }

    $self->subscriber->register_handler( $queues, $handler_args, $handler );
}

# this is a convenience method to configure exchanges, queues, messages and handlers
# with a common set of options. allows one to call all register methods without the
# register_ prefix.
#
# Example:
#  client.configure :exchange => :foobar do |config|
#    config.queue :q1, :key => "foo"
#    config.queue :q2, :key => "bar"
#    config.message :foo
#    config.message :bar
#    config.handler :q1 { puts "got foo"}
#    config.handler :q2 { puts "got bar"}
#  end
# def configure(options={}) #:yields: config
#   yield Configurator.new(self, options)
# end
sub configure {

    # TODO: <plu> I have no fucking idea what this does.
}

# publishes a message. the given options hash is merged with options given on message registration.
# def publish(message_name, data=nil, opts={})
#   message_name = message_name.to_s
#   raise UnknownMessage.new("unknown message #{message_name}") unless messages.include?(message_name)
#   publisher.publish(message_name, data, opts)
# end
sub publish {
    my ( $self, $message_name, $data, $options ) = @_;
    $options ||= {};

    die "unknown message ${message_name}" unless $self->has_message($message_name);

    $self->publisher->publish( $message_name, $data, $options );
}

# sends the given message to one of the configured servers and returns the result of running the associated handler.
#
# unexpected behavior can ensue if the message gets routed to more than one recipient, so be careful.
# def rpc(message_name, data=nil, opts={})
#   message_name = message_name.to_s
#   raise UnknownMessage.new("unknown message #{message_name}") unless messages.include?(message_name)
#   publisher.rpc(message_name, data, opts)
# end
sub rpc {
    my ( $self, $message_name, $data, $options ) = @_;
    $options ||= {};

    die "unknown message ${message_name}" unless $self->has_message($message_name);

    $self->publisher->rpc( $message_name, $data, $options );
}

# purges the given queue on all configured servers
# def purge(queue_name)
#   queue_name = queue_name.to_s
#   raise UnknownQueue.new("unknown queue #{queue_name}") unless queues.include?(queue_name)
#   publisher.purge(queue_name)
# end
sub purge {
    my ( $self, $queue_name ) = @_;

    die "unknown queue ${queue_name}" unless $self->has_queue($queue_name);

    $self->publisher->purge($queue_name);
}

# start listening to a list of messages (default to all registered messages).
# runs the given block before entering the eventmachine loop.
# def listen(messages=self.messages.keys, &block)
#   messages = messages.map(&:to_s)
#   messages.each{|m| raise UnknownMessage.new("unknown message #{m}") unless self.messages.include?(m)}
#   subscriber.listen(messages, &block)
# end
sub listen {
    my ( $self, $messages, $block ) = @_;
    $messages ||= [ $self->message_names ];
    foreach my $message (@$messages) {
        die "unknown message ${message}" unless $self->has_message($message);
    }

    $self->subscriber->listen( $messages, $block );
}

# stops the eventmachine loop
# def stop_listening
#   subscriber.stop!
# end
sub stop_listening {
    my ($self) = @_;
    $self->subscriber->stop;
}

# disconnects the publisher from all servers it's currently connected to
# def stop_publishing
#   publisher.stop
# end
sub stop_publishing {
    my ($self) = @_;
    $self->publisher->stop;
}

# traces all messages received on all queues. useful for debugging message flow.
# def trace(&block)
#   queues.each do |name, opts|
#     opts.merge! :durable => false, :auto_delete => true, :amqp_name => queue_name_for_tracing(name)
#   end
#   register_handler(queues.keys) do |msg|
#     puts "-----===== new message =====-----"
#     puts "SERVER: #{msg.server}"
#     puts "HEADER: #{msg.header.inspect}"
#     puts "MSGID: #{msg.msg_id}"
#     puts "DATA: #{msg.data}"
#   end
#   subscriber.listen(messages.keys, &block)
# end
sub trace {
    my ( $self, $block ) = @_;

    my %queues = $self->all_queues;
    while ( my ( $name, $options ) = each %queues ) {
        $options->{durable}     = 0;
        $options->{auto_delete} = 1;
        $options->{amqp_name}   = _queue_name_for_tracing($name);
    }

    # TODO: <plu> finish this
}

# def queue_name_for_tracing(queue)
#   "trace-#{queue}-#{`hostname`.chomp}-#{$$}"
# end
sub _queue_name_for_tracing {
    my ($queue) = @_;
    return sprintf 'trace-%s-%s-%d', $queue, Sys::Hostname::hostname(), $$;
}

1;
