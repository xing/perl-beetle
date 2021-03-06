use strict;
use warnings;
use Test::Exception;
use Test::MockObject;
use Test::More;
use Sub::Override;

use FindBin qw( $Bin );
use lib ( "$Bin/lib", "$Bin/../lib" );
use Test::Beetle;
use Test::Beetle::Bunny;

BEGIN {
    use_ok('Beetle::Subscriber');
    use_ok('Beetle::Client');
}

{
    my $bunny_listen_called = 0;
    my $override            = Sub::Override->new(
        'Test::Beetle::Bunny::listen' => sub {
            $bunny_listen_called++;
        }
    );

    my $client = Beetle::Client->new(
        config => {
            servers                         => 'xx:3333 xx:3333 xx:5555 xx:6666',
            additional_subscription_servers => 'add:4215',
            mq_class                        => 'Test::Beetle::Bunny',
        }
    );

    my $coderef_called = 0;
    $client->subscriber->listen( [qw(foo bar)], sub { $coderef_called++; } );
    $client->subscriber->listen( [qw(foo bar)], 'invalid' );
    $client->subscriber->listen( [qw(foo bar)], undef );

    is( $coderef_called,      1, 'Coderef passed to method listen got called' );
    is( $bunny_listen_called, 3, 'Method listen on bunny object got called' );

    my $exchanges = $client->subscriber->exchanges_for_messages( [qw(foo)] );
    is_deeply( $exchanges, [], 'No messages defined' );

    $client->register_message( 'foo' => { exchange => 'some-exchange' } );

    $exchanges = $client->subscriber->exchanges_for_messages( [qw(foo)] );
    is_deeply( $exchanges, [qw(some-exchange)], 'Message defined for some-exchange' );

    my $queues = $client->subscriber->queues_for_exchanges( [qw(another-exchange)] );
    is_deeply( $queues, [], 'No queues defined' );

    $client->register_queue( 'some-queue' => { exchange => 'another-exchange' } );

    $queues = $client->subscriber->queues_for_exchanges( [qw(another-exchange)] );
    is_deeply( $queues, [qw(some-queue)], 'Queue defined for another-exchange' );

    $client->subscriber->subscribe_queues( [qw(invalid-queue)] );

    throws_ok { $client->subscriber->subscribe('invalid-queue') }
    qr/no handler for queue invalid-queue/,
      'Subscribing to a non-existent queue throws some error';
}

# test "binding queues should iterate over all servers" do
{
    my $client = Beetle::Client->new( config => {
            mq_class                        => 'Test::Beetle::Bunny',
            servers                         => 'one:1111 two:2222',
            additional_subscription_servers => 'add:4215',
        } );
    my $subscriber = $client->subscriber;
    $client->register_queue('x');
    $client->register_queue('y');
    $client->register_handler( x => sub { } );
    $client->register_handler( y => sub { } );

    my @callstack = ();

    my $o1 = Sub::Override->new(
        'Beetle::Base::PubSub::set_current_server' => sub {
            my ( $self, $server ) = @_;
            push @callstack, { 'Beetle::Base::PubSub::set_current_server' => $server };
        }
    );

    my $o2 = Sub::Override->new(
        'Beetle::Base::PubSub::queue' => sub {
            my ( $self, $queue ) = @_;
            push @callstack, { 'Beetle::Base::PubSub::queue' => $queue };
        }
    );

    $subscriber->bind_queues( [qw(x y)] );

    is_deeply(
        \@callstack,
        [
            { 'Beetle::Base::PubSub::set_current_server' => 'one:1111' },
            { 'Beetle::Base::PubSub::queue'              => 'x' },
            { 'Beetle::Base::PubSub::queue'              => 'y' },
            { 'Beetle::Base::PubSub::set_current_server' => 'two:2222' },
            { 'Beetle::Base::PubSub::queue'              => 'x' },
            { 'Beetle::Base::PubSub::queue'              => 'y' },
            { 'Beetle::Base::PubSub::set_current_server' => 'add:4215' },
            { 'Beetle::Base::PubSub::queue'              => 'x' },
            { 'Beetle::Base::PubSub::queue'              => 'y' }
        ],
        'Callstack is correct'
    );
}

# test "initially there should be no exchanges for the current server" do
{
    my $client = Beetle::Client->new( config => { mq_class => 'Test::Beetle::Bunny', } );
    is_deeply( $client->subscriber->exchanges, {}, 'No exchanges defined' );
}

# test "accessing a given exchange should create it using the config. further access should return the created exchange" do
{
    my $client = Beetle::Client->new( config => { mq_class => 'Test::Beetle::Bunny', } );
    $client->register_exchange( some_exchange => { type => 'topic', durable => 1 } );

    my $create_exchange_called = 0;

    my $o1 = Sub::Override->new( 'Beetle::Subscriber::create_exchange' => sub { $create_exchange_called++ } );

    is( $client->subscriber->exchange('some_exchange'), 0, 'exchange didnt exist yet' );
    is( $client->subscriber->exchange('some_exchange'), 1, 'exchange exists' );
    is( $client->subscriber->exchange('some_exchange'), 1, 'exchange exists' );
    is( $create_exchange_called,                        1, 'create_exchange got only called once' );
}

# test "should create exchanges for all exchanges passed to create_exchanges, for all servers" do
{
    my $client = Beetle::Client->new( config => {
            mq_class                        => 'Test::Beetle::Bunny',
            servers                         => 'one:1111 two:2222',
            additional_subscription_servers => 'add:4215'
        } );
    my $subscriber = $client->subscriber;
    $client->register_queue( 'donald' => { exchange => 'duck' } );
    $client->register_queue('mickey');
    $client->register_queue( 'mouse' => { exchange => 'mickey' } );

    my @callstack = ();

    my $o1 = Sub::Override->new(
        'Beetle::Base::PubSub::set_current_server' => sub {
            my ( $self, $server ) = @_;
            push @callstack, { 'Beetle::Base::PubSub::set_current_server' => $server };
            $self->{server} = $server;
        }
    );

    my $o2 = Sub::Override->new(
        'Beetle::Subscriber::create_exchange' => sub {
            my ( $self, $queue ) = @_;
            push @callstack, { 'Beetle::Subscriber::create_exchange' => $queue };
        }
    );

    $subscriber->create_exchanges( [qw(duck mickey)] );

    is_deeply(
        \@callstack,
        [
            { 'Beetle::Base::PubSub::set_current_server' => 'one:1111' },
            { 'Beetle::Subscriber::create_exchange'    => 'duck' },
            { 'Beetle::Subscriber::create_exchange'    => 'mickey' },
            { 'Beetle::Base::PubSub::set_current_server' => 'two:2222' },
            { 'Beetle::Subscriber::create_exchange'    => 'duck' },
            { 'Beetle::Subscriber::create_exchange'    => 'mickey' },
            { 'Beetle::Base::PubSub::set_current_server' => 'add:4215' },
            { 'Beetle::Subscriber::create_exchange'    => 'duck' },
            { 'Beetle::Subscriber::create_exchange'    => 'mickey' },
        ],
        'Callstack is correct'
    );
}

# test "initially we should have no handlers" do
{
    my $client = Beetle::Client->new( config => { mq_class => 'Test::Beetle::Bunny', } );
    is_deeply( $client->subscriber->handlers, {}, 'initially we should have no handlers' );
}

# test "registering a handler for a queue should store it in the configuration with symbolized option keys" do
{
    my $client = Beetle::Client->new( config => { mq_class => 'Test::Beetle::Bunny', } );
    my $opts = { ack => 1 };
    $client->subscriber->register_handler( 'some_queue', $opts, sub { return 42; } );
    my $handler = $client->subscriber->get_handler('some_queue');
    is_deeply( $handler->{options}, $opts, 'Options set correctly' );
    is( $handler->{code}->(), 42, 'CodeRef set correctly' );
}

# test "exceptions raised from message processing should be ignored" do
{
    my $client = Beetle::Client->new( config => { mq_class => 'Test::Beetle::Bunny', } );
    $client->register_queue('somequeue');
    my $callback = $client->subscriber->create_subscription_callback(
        {
            queue_name      => 'my message',
            amqp_queue_name => 'somequeue',
            handler         => {
                code => sub {
                    die "murks";
                },
                options => {},
            },
            options => { exceptions => 1 },
            mq      => $client->subscriber->mq,
        }
    );

    my $o1 = Sub::Override->new( 'Beetle::Message::process' => sub { die "blah" } );
    my $header = Test::Beetle->header_with_params();
    $header->{body} = Test::MockObject->new->mock( 'payload' => sub { return 'body' } );

    lives_ok { $callback->( $header, 'foo' ) } 'exceptions raised from message processing should be ignored';
}

# test "subscribe should create subscriptions on all queues for all servers" do
{
    my $client = Beetle::Client->new( config => {
            mq_class => 'Test::Beetle::Bunny',
            servers => 'localhost:7777 localhost:6666',
            additional_subscription_servers => 'add:4215'
        } );
    $client->register_message($_) for qw(a b);
    $client->register_queue($_)   for qw(a b);
    $client->register_handler( [qw(a b)] => sub { } );
    my @callstack = ();
    my $o1        = Sub::Override->new(
        'Beetle::Subscriber::subscribe' => sub {
            my ( $self, $queue ) = @_;
            push @callstack, { $self->server => $queue };
        }
    );
    $client->subscriber->subscribe_queues( [qw(a b)] );
    is_deeply(
        \@callstack,
        [
            { 'localhost:7777' => 'a' },
            { 'localhost:7777' => 'b' },
            { 'localhost:6666' => 'a' },
            { 'localhost:6666' => 'b' },
            { 'add:4215'       => 'a' },
            { 'add:4215'       => 'b' }
        ],
        'Callstack is correct'
    );
}

# handler method 'processing_completed' should be called under all circumstances
{
    my $client = Beetle::Client->new( config => { mq_class => 'Test::Beetle::Bunny', } );
    $client->register_queue('somequeue');
    my $completed = 0;
    my $callback = $client->subscriber->create_subscription_callback(
        {
            queue_name      => 'my message',
            amqp_queue_name => 'somequeue',
            handler         => {
                code => sub {},
                options => {
                    completed_callback => sub {
                        $completed++;
                    },
                },
            },
            options => { exceptions => 1 },
            mq      => $client->subscriber->mq,
        }
    );

    my $msg = {
        header => Test::Beetle->header_with_params(),
        body   => Test::MockObject->new->mock( 'payload' => sub { return '{"foo":"bar"}' } ),
    };

    $callback->( $msg );
    is $completed, 1, 'Completed callback called for successful message processing';

    my $o1 = Sub::Override->new( 'Beetle::Message::process' => sub { die "blah" } );
    $callback->( $msg );
    is $completed, 2, 'Completed callback called for internal processing exception'
}

done_testing;
