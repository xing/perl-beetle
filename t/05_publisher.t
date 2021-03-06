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
    use_ok('Beetle::Base::PubSub');
    use_ok('Beetle::Publisher');
    use_ok('Beetle::Client');
}

# test "acccessing a bunny for a server which doesn't have one should create it and associate it with the server" do
{
    my $override = Sub::Override->new( 'Beetle::Publisher::new_bunny' => sub { return 42; } );
    my $client   = Beetle::Client->new;
    my $pub      = Beetle::Publisher->new( client => $client );
    is( $pub->bunny,                     42, 'Method new_bunny works as expected' );
    is( $pub->get_bunny( $pub->server ), 42, 'Bunnies got set correctly' );
}

{
    my $client = Beetle::Client->new( config => { bunny_class => 'Test::Beetle::Bunny' });
    my $pub = Beetle::Publisher->new( client => $client );
    is_deeply( $pub->{bunnies}, {}, 'initially there should be no bunnies' );
}

{
    my $client = Beetle::Client->new( config => { bunny_class => 'Test::Beetle::Bunny' });
    my $pub = Beetle::Publisher->new( client => $client );
    is_deeply( $pub->{dead_servers}, {}, 'initially there should be no dead servers' );
}

{
    local $Beetle::Publisher::RECYCLE_DEAD_SERVERS_DELAY = 1;
    my $client = Beetle::Client->new( config => {
            servers                         => 'localhost:3333 localhost:4444 localhost:5555',
            additional_subscription_servers => 'add:4215',
        } );
    my $publisher = $client->publisher;

    is_deeply( $publisher->servers, [qw(localhost:3333 localhost:4444 localhost:5555)],
        'Servers attribute is correct' );

    my @servers = ();

    for ( 0 .. 2 ) {
        push @servers, $publisher->server;
        $publisher->mark_server_dead;

        isnt( $publisher->server, $servers[$_], 'The dead server moved away' );
        is( defined( $publisher->dead_servers->{ $servers[$_] } ),
            1, 'The dead server got added to the dead servers list' );
    }

    # This should not do anything (yet)
    $publisher->recycle_dead_servers;

    is( $publisher->count_servers,      1, 'One server back' );
    is( $publisher->count_dead_servers, 2, 'All but one servers are in the dead servers list' );

    sleep( $Beetle::Publisher::RECYCLE_DEAD_SERVERS_DELAY + 1 );

    # Now it should recycle the servers
    $publisher->recycle_dead_servers;

    is( $publisher->count_servers,      3, '3 servers back to the servers list' );
    is( $publisher->count_dead_servers, 0, 'No more servers are in the dead servers list' );

    is( $publisher->server, undef, 'No server selected' );

    $publisher->select_next_server;

    like( $publisher->server, qr/localhost:\d{4}/, 'New server selected' );
}

{
    my @servers = ();

    local $Beetle::Publisher::RECYCLE_DEAD_SERVERS_DELAY = 1;

    # If this dies, the publisher will mark the server as dead
    my $o1 = Sub::Override->new(
        'Test::Beetle::Bunny::publish' => sub {
            my ($self) = @_;
            push @servers, sprintf( '%s:%d', $self->host, $self->port );
            die 'dead server';
        }
    );

    my $client = Beetle::Client->new(
        config => {
            servers                         => 'localhost:3333 localhost:4444',
            additional_subscription_servers => 'add:4215',
            bunny_class                     => 'Test::Beetle::Bunny',
        }
    );
    my $publisher = $client->publisher;
    $client->register_queue( mama => { exchange => 'mama-exchange' } );
    $client->register_message( mama => { ttl => 60 * 60, exchange => 'mama-exchange' } );
    $publisher->publish( mama => 'XXX' );

    is_deeply( [ sort @servers ], [qw(localhost:3333 localhost:4444)], 'Server cycling works' );
    is_deeply( $publisher->servers, [], 'Servers attribute is empty as well' );
    is( $publisher->server,             undef, 'No server set because all are dead' );
    is( $publisher->count_dead_servers, 2,     'There are two dead servers now' );

    sleep( $Beetle::Publisher::RECYCLE_DEAD_SERVERS_DELAY + 1 );

    # Override this again to see if the dead servers get recycled properly on a new publish call
    my $o2 = Sub::Override->new( 'Test::Beetle::Bunny::publish' => sub { } );
    $publisher->publish( mama => 'XXX' );

    is_deeply( [ sort @{ $publisher->servers } ], [qw(localhost:3333 localhost:4444)], 'Server recycling works' );
    is( $publisher->count_dead_servers, 0, 'There are no dead servers anymore' );
}

# test "redundant publishing should send the message to two servers" do
{
    my @servers = ();

    my $override = Sub::Override->new(
        'Test::Beetle::Bunny::publish' => sub {
            my ($self) = @_;
            push @servers, sprintf( '%s:%d', $self->host, $self->port );
        }
    );

    my $client = Beetle::Client->new(
        config => {
            servers                         => 'localhost:3333 localhost:4444 localhost:5555',
            additional_subscription_servers => 'add:4215',
            bunny_class                     => 'Test::Beetle::Bunny',
        }
    );
    my $publisher = $client->publisher;

    my $count = $publisher->publish_with_redundancy( 'mama-exchange', 'mama', 'XXX', {} );
    is( $count, scalar(@servers), 'Message got published to two servers' );
}

{
    my @servers = ();

    my $override = Sub::Override->new(
        'Test::Beetle::Bunny::publish' => sub {
            my ($self) = @_;
            push @servers, sprintf( '%s:%d', $self->host, $self->port );
        }
    );

    my $client = Beetle::Client->new(
        config => {
            servers                         => 'localhost:3333',
            additional_subscription_servers => 'add:4215',
            bunny_class                     => 'Test::Beetle::Bunny',
        }
    );
    my $publisher = $client->publisher;

    my $count = $publisher->publish_with_redundancy( 'mama-exchange', 'mama', 'XXX', {} );
    is( $count, scalar(@servers),
        'If there is one server publish_with_redundancy falls back to publish_with_failover' );
}

{
    my @servers = ();

    my $override = Sub::Override->new(
        'Test::Beetle::Bunny::publish' => sub {
            die;
        }
    );

    my $client = Beetle::Client->new(
        config => {
            servers                         => 'localhost:3333 localhost:4444',
            additional_subscription_servers => 'add:4215',
            bunny_class                     => 'Test::Beetle::Bunny',
        }
    );
    my $publisher = $client->publisher;

    my $count = $publisher->publish_with_redundancy( 'mama-exchange', 'mama', 'XXX', {} );
    is( $count, 0, 'Both servers dead, no messages published at all' );
}

{
    my @servers = ();

    my $client = Beetle::Client->new(
        config => {
            servers                         => 'localhost:3333 localhost:4444 localhost:5555 localhost:6666',
            additional_subscription_servers => 'add:4215',
            bunny_class                     => 'Test::Beetle::Bunny',
        }
    );
    my $publisher = $client->publisher;

    my $count = $publisher->publish_with_redundancy( 'mama-exchange', 'mama', 'XXX', {} );
    is( $count, 2, 'Even if there are four servers the message gets published to two of them only' );
}

# This should publish to localhost:3333 and localhost:5555 even though localhost:3333 is listed twice
{
    my $client = Beetle::Client->new(
        config => {
            servers                         => 'xx:3333 xx:3333 xx:5555 xx:6666',
            additional_subscription_servers => 'add:4215',
            bunny_class                     => 'Test::Beetle::Bunny',
        }
    );
    my $publisher = $client->publisher;

    my @servers = $publisher->publish_with_redundancy( 'mama-exchange', 'mama', 'XXX', {} );
    is( scalar(@servers), 2, 'Even if there is a server listed twice it should get published to two unique servers' );
}

# test "redundant publishing should return 1 if the message was published to one server only" do
{
    my @servers = ();

    my $override = Sub::Override->new(
        'Test::Beetle::Bunny::publish' => sub {
            my ($self) = @_;
            die if $self->host ne 'dead';
            push @servers, sprintf( '%s:%d', $self->host, $self->port );
        }
    );

    my $client = Beetle::Client->new(
        config => {
            servers                         => 'dead:3333 alive:4444 dead:5555',
            additional_subscription_servers => 'add:4215',
            bunny_class                     => 'Test::Beetle::Bunny',
        }
    );
    my $publisher = $client->publisher;

    my $count = $publisher->publish_with_redundancy( 'mama-exchange', 'mama', 'XXX', {} );
    is( $count, scalar(@servers), 'Message got published to one server because only one is alive' );
}

# test "redundant publishing should return 0 if the message was published to no server" do
{
    my @servers = ();

    my $override = Sub::Override->new(
        'Test::Beetle::Bunny::publish' => sub {
            my ($self) = @_;
            die if $self->host ne 'dead';
            push @servers, sprintf( '%s:%d', $self->host, $self->port );
        }
    );

    my $client = Beetle::Client->new(
        config => {
            servers                         => 'dead:3333 dead:4444 dead:5555',
            additional_subscription_servers => 'add:4215',
            bunny_class                     => 'Test::Beetle::Bunny',
        }
    );
    my $publisher = $client->publisher;

    my $count = $publisher->publish_with_redundancy( 'mama-exchange', 'mama', 'XXX', {} );
    is( $count, scalar(@servers), 'Message got published to one server because only one is alive' );
}

# test "redundant publishing should publish to two of three servers if one server is dead" do
{
    my @servers = ();

    my $override = Sub::Override->new(
        'Test::Beetle::Bunny::publish' => sub {
            my ($self) = @_;
            die if $self->host ne 'dead';
            push @servers, sprintf( '%s:%d', $self->host, $self->port );
        }
    );

    my $client = Beetle::Client->new(
        config => {
            servers                         => 'dead:3333 alive:4444 alive:5555',
            additional_subscription_servers => 'add:4215',
            bunny_class                     => 'Test::Beetle::Bunny',
        }
    );
    my $publisher = $client->publisher;

    my $count = $publisher->publish_with_redundancy( 'mama-exchange', 'mama', 'XXX', {} );
    is( $count, scalar(@servers), 'Message got published to two servers' );
}

# test "binding a queue should create it using the config and bind it to the exchange with the name specified" do
{

    my @queue_declare = ();
    my @queue_bind    = ();

    my $o1 = Sub::Override->new(
        'Test::Beetle::Bunny::queue_declare' => sub {
            my ( $self, $queue, $options ) = @_;
            push @queue_declare, { queue => $queue, options => $options };
        }
    );

    my $o2 = Sub::Override->new(
        'Test::Beetle::Bunny::queue_bind' => sub {
            my ( $self, $queue, $exchange, $key ) = @_;
            push @queue_bind, { queue => $queue, exchange => $exchange, key => $key };
        }
    );

    my $client = Beetle::Client->new( config => { bunny_class => 'Test::Beetle::Bunny' } );
    $client->register_queue( some_queue => { exchange => 'some_exchange', key => 'some_key' } );
    $client->publisher->queue('some_queue');

    is( $queue_declare[0]->{queue}, 'some_queue', 'Queue name set correctly' );
    is_deeply(
        $queue_declare[0]->{options},
        {
            durable     => 1,
            passive     => 0,
            auto_delete => 0,
            exclusive   => 0
        },
        'All queue_declare options set correctly'
    );

    is_deeply(
        $queue_bind[0],
        {
            'exchange' => 'some_exchange',
            'key'      => 'some_key',
            'queue'    => 'some_queue'
        },
        'All queue_bind options set correctly'
    );
}

# test "accessing a given exchange should create it using the config. further access should return the created exchange" do
{
    my @data = ();

    my $override = Sub::Override->new(
        'Test::Beetle::Bunny::exchange_declare' => sub {
            my ( $self, $exchange, $options ) = @_;
            push @data, { $exchange => $options };
        }
    );

    my $client = Beetle::Client->new( config => { bunny_class => 'Test::Beetle::Bunny' } );
    $client->register_exchange( some_exchange => { type => 'topic', durable => 1 } );

    for ( 1 .. 5 ) {
        $client->publisher->exchange('some_exchange');
    }

    is_deeply(
        \@data,
        [
            {
                'some_exchange' => {
                    'durable' => 1,
                    'type'    => 'topic'
                }
            }
        ],
        'Exchange got created only once'
    );
}

# test "select_next_server should cycle through the list of all servers" do
{
    my $client = Beetle::Client->new(
        config => {
            servers                         => 'localhost:3333 localhost:4444 localhost:5555',
            additional_subscription_servers => 'add:4215',
            bunny_class                     => 'Test::Beetle::Bunny',
        }
    );
    my $publisher = $client->publisher;
    is_deeply( $publisher->servers, [qw(localhost:3333 localhost:4444 localhost:5555)], 'All servers there' );
    ok( $publisher->set_current_server('localhost:3333'), 'Method set_current_server works' );
    is( $publisher->server, 'localhost:3333', 'Correct server set #1' );
    ok( $publisher->select_next_server, 'Select next server #1' );
    is( $publisher->server, 'localhost:4444', 'Correct server set #2' );
    ok( $publisher->select_next_server, 'Select next server #2' );
    is( $publisher->server, 'localhost:5555', 'Correct server set #3' );
    ok( $publisher->select_next_server, 'Select next server #3' );
    is( $publisher->server, 'localhost:3333', 'Correct server set #4' );
}

# test "select_next_server should return 0 if there are no servers to publish to" do
{
    my $client = Beetle::Client->new(
        config => {
            servers                         => 'localhost:3333 localhost:4444 localhost:5555',
            additional_subscription_servers => 'add:4215',
            bunny_class                     => 'Test::Beetle::Bunny',
        }
    );
    my $publisher = $client->publisher;
    $publisher->{servers} = [];
    is( $publisher->select_next_server, 0, 'The method select_next_server returns 0 when no servers are configured' );
}

# test "stop! should shut down bunny and clean internal data structures" do
{
    my $client = Beetle::Client->new(
        config => {
            servers                         => 'localhost:3333',
            additional_subscription_servers => 'add:4215',
            bunny_class                     => 'Test::Beetle::Bunny',
        }
    );
    my $publisher = $client->publisher;

    $client->register_exchange('some_exchange');
    $client->publisher->exchange('some_exchange');

    $client->register_queue( some_queue => { exchange => 'some_exchange', key => 'some_key' } );
    $client->publisher->queue('some_queue');

    is_deeply( $publisher->exchanges, { some_exchange => 1 }, 'Exchange is there' );
    is_deeply( $publisher->queues,    { some_queue    => 1 }, 'Queue is there' );

    $publisher->stop;

    is_deeply( $publisher->exchanges, {}, 'Exchanges got cleaned up' );
    is_deeply( $publisher->queues,    {}, 'Queues got cleaned up' );
}

# test "should bind the defined queues for the used exchanges when publishing" do
{
    my $bind_queue_calls = 0;
    my $override         = Sub::Override->new(
        'Beetle::Publisher::bind_queue' => sub {
            $bind_queue_calls++;
        }
    );

    my $client = Beetle::Client->new(
        config => {
            servers                         => 'localhost:3333',
            additional_subscription_servers => 'add:4215',
            bunny_class                     => 'Test::Beetle::Bunny',
        }
    );
    $client->register_queue( 'test_queue_1' => { exchange => 'test_exchange' } );
    $client->register_queue( 'test_queue_2' => { exchange => 'test_exchange' } );
    $client->register_queue( 'test_queue_3' => { exchange => 'test_exchange_2' } );
    $client->publisher->bind_queues_for_exchange('test_exchange');
    $client->publisher->bind_queues_for_exchange('test_exchange_2');
    is( $bind_queue_calls, 3, 'The method bind_queue got called 3 times' );
}

# test "should not rebind the defined queues for the used exchanges if they already have been bound" do
{
    my $bind_queue_calls = 0;
    my $override         = Sub::Override->new(
        'Beetle::Publisher::bind_queue' => sub {
            $bind_queue_calls++;
        }
    );

    my $client = Beetle::Client->new(
        config => {
            servers                         => 'localhost:3333',
            additional_subscription_servers => 'add:4215',
            bunny_class                     => 'Test::Beetle::Bunny',
        }
    );
    $client->register_queue( 'test_queue_1' => { exchange => 'test_exchange' } );
    $client->register_queue( 'test_queue_2' => { exchange => 'test_exchange' } );
    $client->publisher->bind_queues_for_exchange('test_exchange');
    $client->publisher->bind_queues_for_exchange('test_exchange');
    is( $bind_queue_calls, 2, 'The method bind_queue got called 2 times' );
}

# test "call the queue binding method when publishing" do
{
    my @exchange_args = ();
    my $o1            = Sub::Override->new(
        'Beetle::Base::PubSub::exchange' => sub {
            my $self = shift;
            push @exchange_args, @_;
        }
    );

    my @bind_queues_for_exchange_args = ();
    my $o2                            = Sub::Override->new(
        'Beetle::Publisher::bind_queues_for_exchange' => sub {
            my $self = shift;
            push @bind_queues_for_exchange_args, @_;
        }
    );

    my $client = Beetle::Client->new(
        config => {
            servers                         => 'localhost:3333',
            additional_subscription_servers => 'add:4215',
            bunny_class                     => 'Test::Beetle::Bunny',
        }
    );
    $client->register_queue( 'mama' => { exchange => 'mama-exchange' } );
    $client->register_message( 'mama' => { ttl => 60 * 60, exchange => 'mama-exchange' } );
    $client->publish( 'mama' => 'XXX' );

    my $exp = ['mama-exchange'];

    is_deeply( \@exchange_args,                 $exp, 'Correct args to exchange call given' );
    is_deeply( \@bind_queues_for_exchange_args, $exp, 'Correct args to bind_queues_for_exchange call given' );
}

# test "purging a queue should purge the queues on all servers" do
{
    my @callstack = ();

    my $o1 = Sub::Override->new(
        'Beetle::Base::PubSub::set_current_server' => sub {
            push @callstack, 'set_current_server';
        }
    );

    my $o2 = Sub::Override->new(
        'Beetle::Base::PubSub::queue' => sub {
            my $self = shift;
            push @callstack, { method => 'queue', args => \@_ };
        }
    );

    my $o3 = Sub::Override->new(
        'Test::Beetle::Bunny::purge' => sub {
            push @callstack, 'purge';
        }
    );

    my $client = Beetle::Client->new(
        config => {
            servers                         => 'localhost:3333 localhost:4444',
            additional_subscription_servers => 'add:4215',
            bunny_class                     => 'Test::Beetle::Bunny',
        }
    );
    $client->publisher->purge('some_queue');

    is_deeply(
        \@callstack,
        [
            'set_current_server',
            {
                'args'   => ['some_queue'],
                'method' => 'queue'
            },
            'purge',
            'set_current_server',
            {
                'args'   => ['some_queue'],
                'method' => 'queue'
            },
            'purge'
        ],
        'Callstack is correct'
    );
}

# test "stop should shut down all bunnies" do
{
    my @callstack = ();

    my $o1 = Sub::Override->new(
        'Beetle::Base::PubSub::set_current_server' => sub {
            my $self = shift;
            push @callstack, { method => 'set_current_server', args => \@_ };
        }
    );

    my $client = Beetle::Client->new( config => {
            bunny_class                     => 'Test::Beetle::Bunny',
            servers                         => 'localhost:3333 localhost:4444',
            additional_subscription_servers => 'add:4215',
     } );

    $client->stop_publishing;

    is_deeply(
        \@callstack,
        [
            {
                'args'   => ['localhost:3333'],
                'method' => 'set_current_server'
            },
            {
                'args'   => ['localhost:4444'],
                'method' => 'set_current_server'
            },
        ],
        'Callstack is correct'
    );
}

# test "publishing should use the message ttl passed in the options hash to encode the message body" do
# test "publishing with redundancy should use the message ttl passed in the options hash to encode the message body" do
{
    my $opts = { ttl => 60 * 60 * 24, message_id => 123, };
    my @callstack = ();

    my $o1 = Sub::Override->new(
        'Beetle::Message::publishing_options' => sub {
            my ( $package, %args ) = @_;
            is_deeply( \%args, $opts, 'Got correct args in publishing_options' );
            push @callstack, 'Beetle::Message::publishing_options';
            return \%args;
        }
    );

    my $o2 = Sub::Override->new(
        'Beetle::Publisher::select_next_server' => sub {
            push @callstack, 'Beetle::Publisher::select_next_server';
        }
    );

    my $o3 = Sub::Override->new(
        'Beetle::Base::PubSub::exchange' => sub {
            push @callstack, 'Beetle::Base::PubSub::exchange';
        }
    );

    foreach my $method (qw(publish_with_failover publish_with_redundancy)) {
        my $client = Beetle::Client->new( config => { bunny_class => 'Test::Beetle::Bunny' } );

        my $result = $client->publisher->$method( 'mama-exchange', 'mama', 'data', $opts );

        is( $result, 1, 'Return value of publish_with_failover is correct' );

        is_deeply(
            \@callstack,
            [
                qw(
                  Beetle::Message::publishing_options
                  Beetle::Publisher::select_next_server
                  Beetle::Base::PubSub::exchange
                  )
            ],
            'Got correct callstack'
        );

        @callstack = ();
    }
}

# test "initially there should be no queues for the current server" do
{
    my $client = Beetle::Client->new( config => { bunny_class => 'Test::Beetle::Bunny' } );
    is_deeply( $client->publisher->queues, {}, 'No queues there yet' );
}

# test "initially there should be no exchanges for the current server" do
{
    my $client = Beetle::Client->new( config => { bunny_class => 'Test::Beetle::Bunny' } );
    is_deeply( $client->publisher->exchanges, {}, 'No exchanges there yet' );
}

# test "options for publish take precedence over message definition" do
{
    my $client = Beetle::Client->new( config => { bunny_class => 'Test::Beetle::Bunny' } );
    $client->register_message( msg => { exchange => 'some-exchange', key => 'default-key' } );
    my $override = Sub::Override->new('Beetle::Publisher::publish_with_failover' => sub {
            my ( $self, $exchange_name, $message_name, $data, $options ) = @_;
            is ( $options->{key}, "overridden-key", "Publish options take precedence over message definition");
        }
    );
    $client->publish('msg', { some => 'data' }, { key => 'overridden-key'});
}

done_testing;
