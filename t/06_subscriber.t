use strict;
use warnings;
use Test::Exception;
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
            servers     => 'xx:3333 xx:3333 xx:5555 xx:6666',
            bunny_class => 'Test::Beetle::Bunny',
        }
    );

    my $coderef_called = 0;
    $client->subscriber->listen( [qw(foo bar)], sub { $coderef_called++; } );

    is( $coderef_called,      1, 'Coderef passed to method listen got called' );
    is( $bunny_listen_called, 1, 'Method listen on bunny object got called' );

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
    my $client = Beetle::Client->new( config => { bunny_class => 'Test::Beetle::Bunny', } );
    my $subscriber = $client->subscriber;
    $client->register_queue('x');
    $client->register_queue('y');
    $client->register_handler( x => sub { } );
    $client->register_handler( y => sub { } );
    $subscriber->{servers} = [qw(one:1111 two:2222)];

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
            { 'Beetle::Base::PubSub::queue'              => 'y' }
        ],
        'Callstack is correct'
    );
}

# test "initially there should be no exchanges for the current server" do
{
    my $client = Beetle::Client->new( config => { bunny_class => 'Test::Beetle::Bunny', } );
    is_deeply( $client->subscriber->exchanges, {}, 'No exchanges defined' );
}

# test "accessing a given exchange should create it using the config. further access should return the created exchange" do
{
    my $client = Beetle::Client->new( config => { bunny_class => 'Test::Beetle::Bunny', } );
    $client->register_exchange( some_exchange => { type => 'topic', durable => 1 } );

    my $create_exchange_called = 0;

    my $o1 = Sub::Override->new( 'Beetle::Base::PubSub::create_exchange' => sub { $create_exchange_called++ } );

    is( $client->subscriber->exchange('some_exchange'), 0, 'exchange didnt exist yet' );
    is( $client->subscriber->exchange('some_exchange'), 1, 'exchange exists' );
    is( $client->subscriber->exchange('some_exchange'), 1, 'exchange exists' );
    is( $create_exchange_called,                        1, 'create_exchange got only called once' );
}

# test "should create exchanges for all exchanges passed to create_exchanges, for all servers" do
{
    my $client = Beetle::Client->new( config => { bunny_class => 'Test::Beetle::Bunny', } );
    my $subscriber = $client->subscriber;
    $client->register_queue( 'donald' => { exchange => 'duck' } );
    $client->register_queue('mickey');
    $client->register_queue( 'mouse' => { exchange => 'mickey' } );
    $subscriber->{servers} = [qw(one:1111 two:2222)];

    my @callstack = ();

    my $o1 = Sub::Override->new(
        'Beetle::Base::PubSub::set_current_server' => sub {
            my ( $self, $server ) = @_;
            push @callstack, { 'Beetle::Base::PubSub::set_current_server' => $server };
            $self->{server} = $server;
        }
    );

    my $o2 = Sub::Override->new(
        'Beetle::Base::PubSub::create_exchange' => sub {
            my ( $self, $queue ) = @_;
            push @callstack, { 'Beetle::Base::PubSub::create_exchange' => $queue };
        }
    );

    $subscriber->create_exchanges( [qw(duck mickey)] );

    is_deeply(
        \@callstack,
        [
            { 'Beetle::Base::PubSub::set_current_server' => 'one:1111' },
            { 'Beetle::Base::PubSub::create_exchange'    => 'duck' },
            { 'Beetle::Base::PubSub::create_exchange'    => 'mickey' },
            { 'Beetle::Base::PubSub::set_current_server' => 'two:2222' },
            { 'Beetle::Base::PubSub::create_exchange'    => 'duck' },
            { 'Beetle::Base::PubSub::create_exchange'    => 'mickey' },
        ],
        'Callstack is correct'
    );
}

done_testing;
