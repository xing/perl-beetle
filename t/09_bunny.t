use strict;
use warnings;
use Test::Exception;
use Test::More;
use Sub::Override;

use FindBin qw( $Bin );
use lib ( "$Bin/lib", "$Bin/../lib" );
use Test::Beetle;
use Test::MockObject;

BEGIN {
    use_ok('Beetle::Bunny');
    use_ok('AnyEvent::RabbitMQ::Channel');
    use_ok('AnyEvent::RabbitMQ');
}

# Make Devel::Cover happy

AnyEvent::RabbitMQ::Channel::DESTROY();
AnyEvent::RabbitMQ::DESTROY();

# {
#     my $options;
#     my $o1 = Sub::Override->new(
#         'Beetle::Bunny::_ack' => sub {
#             my ( $self, %args ) = @_;
#             $options = \%args;
#         }
#     );
#     my $bunny = Beetle::Bunny->new( port => 5672, host => 'localhost' );
# 
#     $bunny->ack;
#     is_deeply( $options, {}, 'options got default value' );
# 
#     $bunny->ack( { foo => 'bar' } );
#     is_deeply( $options, { foo => 'bar' }, 'options got set correctly' );
# }

# {
#     my $options;
#     my $o1 = Sub::Override->new(
#         'Beetle::Bunny::_declare_exchange' => sub {
#             my ( $self, %args ) = @_;
#             $options = \%args;
#         }
#     );
#     my $bunny = Beetle::Bunny->new( port => 5672, host => 'localhost' );
# 
#     $bunny->exchange_declare('ex1');
#     is_deeply( $options, { exchange => 'ex1', no_ack => 0 }, 'options got default value' );
# 
#     $bunny->exchange_declare( 'ex1' => { ex => 'tra' } );
#     is_deeply( $options, { exchange => 'ex1', no_ack => 0, ex => 'tra' }, 'options got set correctly' );
# }

# {
#     my $args;
#     my $o1 = Sub::Override->new(
#         'Beetle::Bunny::_publish' => sub {
#             my ( $self, %a ) = @_;
#             $args = \%a;
#         }
#     );
#     my $bunny = Beetle::Bunny->new( port => 5672, host => 'localhost' );
# 
#     $bunny->publish( 'ex1', 'message1', 'data1' );
#     is_deeply(
#         $args,
#         {
#             'body'        => 'data1',
#             'exchange'    => 'ex1',
#             'header'      => {},
#             'no_ack'      => 0,
#             'routing_key' => 'message1'
#         },
#         'args got default value'
#     );
# 
#     $bunny->publish( 'ex1', 'message1', 'data1', { some => 'header' } );
#     is_deeply(
#         $args,
#         {
#             'body'        => 'data1',
#             'exchange'    => 'ex1',
#             'header'      => { 'some' => 'header' },
#             'no_ack'      => 0,
#             'routing_key' => 'message1'
#         },
#         'args got set correctly'
#     );
# }

# {
#     my $options;
#     my $o1 = Sub::Override->new(
#         'Beetle::Bunny::_purge_queue' => sub {
#             my ( $self, %args ) = @_;
#             $options = \%args;
#         }
#     );
#     my $bunny = Beetle::Bunny->new( port => 5672, host => 'localhost' );
# 
#     $bunny->purge('q1');
#     is_deeply( $options, { queue => 'q1' }, 'options got default value' );
# 
#     $bunny->purge( 'q1' => { ex => 'tra' } );
#     is_deeply( $options, { queue => 'q1', ex => 'tra' }, 'options got set correctly' );
# }
# 
# {
#     my @callstack = ();
#     my $options;
#     my $o1 = Sub::Override->new(
#         'Beetle::Bunny::_build__mq' => sub {
#             return Test::MockObject->new->mock(
#                 'open_channel' => sub {
#                     my $o = Test::MockObject->new;
#                     $o->mock( declare_exchange => sub { push @callstack, 'declare_exchange'; } );
#                     $o->mock( declare_queue    => sub { push @callstack, 'declare_queue'; } );
#                     $o->mock( bind_queue       => sub { push @callstack, 'bind_queue'; } );
#                     $o->mock( consume          => sub { push @callstack, 'consume'; } );
#                     return $o;
#                 }
#             );
#         }
#     );
#     my $bunny = Beetle::Bunny->new( port => 5672, host => 'localhost' );
# 
#     my $coderef = sub { };
# 
#     $bunny->exchange_declare('exchange1');
#     $bunny->queue_declare('queue1');
#     $bunny->queue_bind( 'queue1', 'exchange1', 'key1' );
#     $bunny->subscribe( 'queue1', $coderef );
# 
#     is_deeply(
#         $bunny->_command_history,
#         [
#             { 'exchange_declare' => ['exchange1'] },
#             { 'queue_declare'    => ['queue1'] },
#             { 'queue_bind'       => [ 'queue1', 'exchange1', 'key1' ] },
#             { 'subscribe' => [ 'queue1', $coderef ] }
#         ],
#         'Command history set correctly'
#     );
# 
#     is_deeply(
#         \@callstack,
#         [qw(declare_exchange declare_queue bind_queue consume)],
#         'Callstack before reconnect call is correct'
#     );
# 
#     ok( $bunny->_reconnect, 'Reconnect...' );
#     ok( $bunny->_reconnect, 'Reconnect...' );
# 
#     is_deeply(
#         \@callstack,
#         [
#             qw(
#               declare_exchange declare_queue bind_queue consume
#               declare_exchange declare_queue bind_queue consume
#               declare_exchange declare_queue bind_queue consume
#               )
#         ],
#         'Callstack after reconnect call is correct'
#     );
# 
# }

done_testing;
