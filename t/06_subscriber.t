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

done_testing;
