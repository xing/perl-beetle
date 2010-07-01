use strict;
use warnings;
use Test::Exception;
use Test::More;
use Sub::Override;

use FindBin qw( $Bin );
use lib ( "$Bin/lib", "$Bin/../lib" );
use Test::Beetle;

BEGIN {
    use_ok('Beetle::Client');
}

{
    my $client = Beetle::Client->new;
    is_deeply( $client->servers, ['localhost:5672'], 'should have a default server' );
    is_deeply( $client->exchanges, {}, 'should have no exchanges' );
    is_deeply( $client->queues,    {}, 'should have no queues' );
    is_deeply( $client->messages,  {}, 'should have no messages' );
    is_deeply( $client->bindings,  {}, 'should have no bindings' );
}

{
    my $client = Beetle::Client->new;
    my $options = { durable => 0, type => 'fanout' };
    $client->register_exchange( some_exchange => $options );
    is_deeply(
        $client->get_exchange('some_exchange'),
        { durable => 1, type => 'topic' },
        'registering an exchange should store it in the configuration with'
          . ' symbolized option keys and force a topic queue and durability'
    );

    throws_ok { $client->register_exchange('some_exchange') }
    qr/exchange some_exchange already configured/,
      'registering an exchange should raise a configuration error if it is already configured';
}

{
    my $client = Beetle::Client->new;
    $client->register_queue( some_queue => { durable => 0, exchange => 'some_exchange' } );
    is( $client->has_exchange('some_exchange'),
        1, "registering a queue should automatically register the corresponding exchange if it doesn't exist yet" );
}

{
    my $client = Beetle::Client->new;
    $client->register_queue( some_queue => { key => 'some_key', exchange => 'some_exchange' } );
    my $bindings = $client->get_binding('some_queue');
    is_deeply(
        $bindings,
        [ { key => 'some_key', exchange => 'some_exchange' } ],
        'registering a queue should store key and exchange in the bindings list'
    );
}

{
    my $client = Beetle::Client->new;
    $client->register_queue( some_queue => { key => 'some_key', exchange => 'some_exchange' } );
    $client->register_binding( some_queue => { key => 'other_key', exchange => 'other_exchange' } );
    my $bindings = $client->get_binding('some_queue');
    my $expected =
      [ { key => 'some_key', exchange => 'some_exchange' }, { key => 'other_key', exchange => 'other_exchange' } ];
    is_deeply( $bindings, $expected,
        'registering an additional binding for a queue should store key and exchange in the bindings list' );
}

{
    my $client = Beetle::Client->new;
    $client->register_queue( some_queue => { durable => 0, exchange => 'some_exchange' } );
    my $queue = $client->get_queue('some_queue');
    is_deeply(
        $queue,
        { durable => 1, passive => 0, auto_delete => 0, exclusive => 0, amqp_name => 'some_queue' },
        'registering a queue should store it in the configuration with symbolized'
          . ' option keys and force durable=true and passive=false and set the amqp queue name'
    );
}

{
    my $client = Beetle::Client->new;
    $client->register_queue( some_queue => { durable => 1, exchange => 'some_exchange' } );
    my $exchange = $client->get_exchange('some_exchange');
    is_deeply( $exchange->{queues}, [qw(some_queue)],
        "registering a queue should add the queue to the list of queues of the queue's exchange" );
}

{
    my $client = Beetle::Client->new;
    $client->register_queue( queue1 => { exchange => 'some_exchange' } );
    $client->register_queue( queue2 => { exchange => 'some_exchange' } );
    my $exchange = $client->get_exchange('some_exchange');
    is_deeply( $exchange->{queues}, [qw(queue1 queue2)],
        "registering two queues should add both queues to the list of queues of the queue's exchange" );
}

{
    my $client = Beetle::Client->new;
    $client->register_queue( queue1 => { durable => 1, exchange => 'some_exchange' } );
    throws_ok { $client->register_queue( queue1 => { durable => 1, exchange => 'some_exchange' } ); }
    qr/queue queue1 already configured/,
      'registering a queue should raise a configuration error if it is already configured';
}

{
    my $client = Beetle::Client->new;
    my $options = { persistent => 1, queue => 'some_queue', exchange => 'some_exchange' };
    $client->register_queue( some_queue => { exchange => 'some_exchange' } );
    $client->register_message( some_message => $options );
    my $message = $client->get_message('some_message');
    my $expected = { persistent => 1, queue => 'some_queue', exchange => 'some_exchange', key => 'some_message' };
    is_deeply( $message, $expected,
        'registering a message should store it in the configuration with symbolized option keys' );
}

{
    my $client = Beetle::Client->new;
    my $options = { persistent => 1, queue => 'some_queue' };
    $client->register_queue( some_queue => { exchange => 'some_exchange' } );
    $client->register_message( some_message => $options );
    throws_ok { $client->register_message( some_message => $options ) }
    qr/message some_message already configured/,
      'registering a message should raise a configuration error if it is already configured';
}

{
    my $client = Beetle::Client->new;
    $client->register_queue( some_queue => { exchange => 'some_exchange' } );
    $client->register_handler( [qw(some_queue)], sub { } );
}

{
    my $client = Beetle::Client->new;
    throws_ok {
        $client->register_handler( [qw(some_queue)], sub { } );
    }
    qr/unknown queue: some_queue/, 'registering a handler for a unknown queue should throw an error';
}

{
    my $client = Beetle::Client->new;
    throws_ok {
        $client->register_handler( [qw(some_queue)], sub { } );
    }
    qr/unknown queue: some_queue/, 'registering a handler for a unknown queue should throw an error';
}

{
    my $client = Beetle::Client->new;
    throws_ok {
        $client->purge('some_queue');
    }
    qr/unknown queue some_queue/, 'purging an unknown should throw an error';
}

{
    my $client = Beetle::Client->new;
    throws_ok {
        $client->publish( some_message => 'blah', {} );
    }
    qr/unknown message some_message/, 'publishing an unknown message should throw an error';
}

{
    my $client = Beetle::Client->new;
    throws_ok {
        $client->listen( [qw(some_message)] );
    }
    qr/unknown message some_message/, 'listening for an unknown message should throw an error';
}

{
    my $client = Beetle::Client->new;
    lives_ok { $client->register_binding( 'some_queue' => {} ); } 'first call to register_binding';
    lives_ok { $client->register_binding( 'some_queue' => {} ); } 'second call to register_binding';
}

{
    my $client = Beetle::Client->new;
    is( $client->{publisher},  undef, 'instantiating a client should not instantiate the publisher' );
    is( $client->{subscriber}, undef, 'instantiating a client should not instantiate the subscriber' );
}

{
    my $client = Beetle::Client->new;
    $client->register_queue('superman');
    $client->register_message('superman');
    $client->register_handler( 'superman' => sub { } );
    isa_ok( $client->{subscriber}, 'Beetle::Subscriber', 'should instantiate a subscriber when used for subscribing' );
}

{
    my $client = Beetle::Client->new;
    $client->register_message('foobar');
    $client->publish( 'foobar' => 'payload' );
    isa_ok( $client->{publisher}, 'Beetle::Publisher', 'should instantiate a publisher when used for publishing' );
}

{
    my $client = Beetle::Client->new;
    $client->register_message('deadletter');
    my @args     = ( 'deadletter' => 'x', { a => 1 } );
    my @result   = ();
    my $override = Sub::Override->new(
        'Beetle::Publisher::publish' => sub {
            my $self = shift;
            @result = @_;
            return 42;
        }
    );
    is( $client->publish(@args), 42, 'Got correct return value' );
    is_deeply( \@result, \@args, 'should delegate publishing to the publisher instance' );
}

{
    my $client   = Beetle::Client->new;
    my $called   = 0;
    my $override = Sub::Override->new( 'Beetle::Publisher::stop' => sub { $called++ } );
    $client->stop_publishing;
    is( $called, 1, 'should delegate stop_publishing to the publisher instance' );
}

{
    my $client = Beetle::Client->new;
    my @result = ();

    my $override = Sub::Override->new(
        'Beetle::Publisher::purge' => sub {
            my $self = shift;
            @result = @_;
            return 42;
        }
    );

    $client->register_queue('queue');
    is( $client->purge('queue'), 42, 'correct return value' );

    is_deeply( \@result, ['queue'], 'should delegate queue purging to the publisher instance' );
}

{
    my $client = Beetle::Client->new;
    for (qw(a b)) {
        $client->register_queue($_);
        $client->register_message($_);
    }

    my $override = Sub::Override->new(
        'Beetle::Subscriber::listen' => sub {
            my ( $self, $messages, $code ) = @_;
            return 42;
        }
    );

    my $result = $client->listen( [qw(a b)] );
    is( $result, 42, 'should delegate listening to the subscriber instance' );
}

{
    my $client   = Beetle::Client->new;
    my $override = Sub::Override->new( 'Beetle::Subscriber::stop' => sub { return 42; } );
    my $result   = $client->stop_listening;
    is( $result, 42, 'should delegate stop_listening to the subscriber instance' );
}

{
    my $client = Beetle::Client->new;
    my $override = Sub::Override->new( 'Beetle::Subscriber::register_handler' => sub { return 42; } );
    $client->register_queue('huhu');
    my $result = $client->register_handler( huhu => sub { } );
    is( $result, 42, 'should delegate handler registration to the subscriber instance' );
}

done_testing;
