use strict;
use warnings;
use Test::Exception;
use Test::More;
use Sub::Override;

use FindBin qw( $Bin );
use lib ( "$Bin/lib", "$Bin/../lib" );
use Test::Beetle;

BEGIN {
    use_ok('Beetle::Bunny');
    use_ok('AnyEvent::RabbitMQ::Channel');
    use_ok('AnyEvent::RabbitMQ');
}

# Make Devel::Cover happy

AnyEvent::RabbitMQ::Channel::DESTROY();
AnyEvent::RabbitMQ::DESTROY();

{
    my $options;
    my $o1 = Sub::Override->new(
        'Beetle::Bunny::_ack' => sub {
            my ( $self, %args ) = @_;
            $options = \%args;
        }
    );
    my $bunny = Beetle::Bunny->new( port => 5672, host => 'localhost' );

    $bunny->ack;
    is_deeply( $options, {}, 'options got default value' );

    $bunny->ack( { foo => 'bar' } );
    is_deeply( $options, { foo => 'bar' }, 'options got set correctly' );
}

{
    my $options;
    my $o1 = Sub::Override->new(
        'Beetle::Bunny::_declare_exchange' => sub {
            my ( $self, %args ) = @_;
            $options = \%args;
        }
    );
    my $bunny = Beetle::Bunny->new( port => 5672, host => 'localhost' );

    $bunny->exchange_declare('ex1');
    is_deeply( $options, { exchange => 'ex1', no_ack => 0 }, 'options got default value' );

    $bunny->exchange_declare( 'ex1' => { ex => 'tra' } );
    is_deeply( $options, { exchange => 'ex1', no_ack => 0, ex => 'tra' }, 'options got set correctly' );
}

{
    my $args;
    my $o1 = Sub::Override->new(
        'Beetle::Bunny::_publish' => sub {
            my ( $self, %a ) = @_;
            $args = \%a;
        }
    );
    my $bunny = Beetle::Bunny->new( port => 5672, host => 'localhost' );

    $bunny->publish( 'ex1', 'message1', 'data1' );
    is_deeply(
        $args,
        {
            'body'        => 'data1',
            'exchange'    => 'ex1',
            'header'      => {},
            'no_ack'      => 0,
            'routing_key' => 'message1'
        },
        'args got default value'
    );

    $bunny->publish( 'ex1', 'message1', 'data1', { some => 'header' } );
    is_deeply(
        $args,
        {
            'body'        => 'data1',
            'exchange'    => 'ex1',
            'header'      => { 'some' => 'header' },
            'no_ack'      => 0,
            'routing_key' => 'message1'
        },
        'args got set correctly'
    );
}

{
    my $options;
    my $o1 = Sub::Override->new(
        'Beetle::Bunny::_purge_queue' => sub {
            my ( $self, %args ) = @_;
            $options = \%args;
        }
    );
    my $bunny = Beetle::Bunny->new( port => 5672, host => 'localhost' );

    $bunny->purge('q1');
    is_deeply( $options, { queue => 'q1' }, 'options got default value' );

    $bunny->purge( 'q1' => { ex => 'tra' } );
    is_deeply( $options, { queue => 'q1', ex => 'tra' }, 'options got set correctly' );
}

done_testing;
