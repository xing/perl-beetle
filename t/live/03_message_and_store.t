use strict;
use warnings;
use Test::More;
use Sub::Override;

use FindBin qw( $Bin );
use lib ( "$Bin/../lib", "$Bin/../../lib" );
use Test::Beetle;
use Test::Beetle::Redis;
use Beetle::Message;
use Beetle::Handler;
use Beetle::Constants;

test_redis(
    sub {
        my $store = shift;

        my $empty_handler = Beetle::Handler->create( sub { } );

        # test "should be able to extract msg_id from any key" do
        {
            $store->flushdb;
            my $header = Test::Beetle->header_with_params();
            my $m      = Beetle::Message->new(
                body   => 'foo',
                header => $header,
                queue  => "somequeue",
                store  => $store
            );
            my @keys = $store->keys( $m->msg_id );
            foreach my $key (@keys) {
                is( $m->msg_id, $store->msg_id($key), 'should be able to extract msg_id from any key' );
            }
        }

        # test "should be able to garbage collect expired keys" do
        {
            $store->flushdb;
            my $override = Sub::Override->new( 'Beetle::Config::gc_threshold' => sub { return 0; } );
            my $header = Test::Beetle->header_with_params( ttl => 0 );
            my $m = Beetle::Message->new(
                body   => 'foo',
                header => $header,
                queue  => "somequeue",
                store  => $store
            );
            is( $m->key_exists, 0, 'Key did not exist yet' );
            is( $m->key_exists, 1, 'Key exists' );

            is( scalar( $store->redis->keys('*') ), 2, 'Keys are really in store (status + expires)' );
            ok( $store->garbage_collect_keys( time + 1 ), 'Garbage collection' );
            is( scalar( $store->redis->keys('*') ), undef, 'Keys have been removed from store' );
        }

        # test "should not garbage collect not yet expired keys" do
        {
            $store->flushdb;
            my $override = Sub::Override->new( 'Beetle::Config::gc_threshold' => sub { return 0; } );
            my $header = Test::Beetle->header_with_params( ttl => 60 );
            my $m = Beetle::Message->new(
                body   => 'foo',
                header => $header,
                queue  => "somequeue",
                store  => $store
            );
            is( $m->key_exists, 0, 'Key did not exist yet' );
            is( $m->key_exists, 1, 'Key exists' );

            is( scalar( $store->redis->keys('*') ), 2, 'Keys are really in store (status + expires)' );
            ok( $store->garbage_collect_keys( time + 1 ), 'Garbage collection' );
            is( scalar( $store->redis->keys('*') ), 2, 'Keys are still store' );
            ok( $store->garbage_collect_keys( time + 61 ), 'Garbage collection' );
            is( scalar( $store->redis->keys('*') ), undef, 'Keys have been removed from store' );
        }

        # test "successful processing of a non redundant message should delete all keys from the database" do
        {
            my $header = Test::Beetle->header_with_params();
            my $m      = Beetle::Message->new(
                body   => 'foo',
                header => $header,
                queue  => "somequeue",
                store  => $store
            );
            is( $m->expired,   0, 'Message is not expired yet' );
            is( $m->redundant, 0, 'Message is not redundant' );

            my $result = $m->process($empty_handler);

            foreach my $key ( $store->keys( $m->msg_id ) ) {
                is( $store->redis->exists($key), 0, "Key $key is not in store anymore" );
            }
        }

        # test "succesful processing of a redundant message twice should delete all keys from the database" do
        {
            my $header = Test::Beetle->header_with_params( redundant => 1 );
            my $m = Beetle::Message->new(
                body   => 'foo',
                header => $header,
                queue  => "somequeue",
                store  => $store
            );
            is( $m->expired,   0, 'Message is not expired yet' );
            is( $m->redundant, 1, 'Message is redundant' );

            $m->process($empty_handler);
            $m->process($empty_handler);

            foreach my $key ( $store->keys( $m->msg_id ) ) {
                is( $store->redis->exists($key), 0, "Key $key is removed from store after 2nd process call" );
            }
        }

        # test "successful processing of a redundant message once should insert all but the delay key and the
        # exception count key into the database" do
        {
            my $header = Test::Beetle->header_with_params( redundant => 1 );
            my $m = Beetle::Message->new(
                body   => 'foo',
                header => $header,
                queue  => "somequeue",
                store  => $store
            );
            is( $m->expired,   0, 'Message is not expired yet' );
            is( $m->redundant, 1, 'Message is redundant' );

            $m->process($empty_handler);

            is( $store->exists( $m->msg_id => 'status' ),     1, 'Key status exists for msg' );
            is( $store->exists( $m->msg_id => 'expires' ),    1, 'Key expires exists for msg' );
            is( $store->exists( $m->msg_id => 'attempts' ),   1, 'Key attempts exists for msg' );
            is( $store->exists( $m->msg_id => 'timeout' ),    1, 'Key timeout exists for msg' );
            is( $store->exists( $m->msg_id => 'ack_count' ),  1, 'Key ack_count exists for msg' );
            is( $store->exists( $m->msg_id => 'delay' ),      0, 'Key delay does not exist for msg' );
            is( $store->exists( $m->msg_id => 'exceptions' ), 0, 'Key exceptions does not exist for msg' );
        }

        # test "an expired message should be acked without calling the handler" do
        {
            my $header = Test::Beetle->header_with_params( ttl => -1 );
            my $m = Beetle::Message->new(
                body   => 'foo',
                header => $header,
                queue  => "somequeue",
                store  => $store
            );
            is( $m->expired, 1, 'Message is expired' );
            my $processed = 0;
            $m->process( sub { $processed = 1; } );
            is( $processed, 0, 'Expired message did not get processed' );
        }

        # test "a delayed message should not be acked and the handler should not be called" do
        {
            my $header = Test::Beetle->header_with_params();
            my $m      = Beetle::Message->new(
                attempts => 2,
                body     => 'foo',
                header   => $header,
                queue    => "somequeue",
                store    => $store,
            );
            $m->set_delay;
            is( $m->key_exists, 0, 'Key did not exist yet' );
            is( $m->delayed,    1, 'Message is delayed' );
            my $processed = 0;
            $m->process( sub { $processed = 1; } );
            is( $processed, 0, 'Delayed message did not get processed' );
        }

        # test "acking a non redundant message should remove the ack_count key" do
        {
            my $header = Test::Beetle->header_with_params();
            my $m      = Beetle::Message->new(
                body   => 'foo',
                header => $header,
                queue  => "somequeue",
                store  => $store,
            );
            $m->process($empty_handler);
            is( $m->redundant, 0, 'Message is not redundant' );
            is( $store->exists( $m->msg_id, 'ack_count' ), 0, 'The key ack_count does not exist in store' );
        }

        # test "a redundant message should be acked after calling the handler" do
        {

            # TODO: <plu> hmm I think this test is crap. Talk to rubys about it.
            my $header = Test::Beetle->header_with_params( redundant => 1 );
            my $m = Beetle::Message->new(
                body   => 'foo',
                header => $header,
                queue  => "somequeue",
                store  => $store,
            );
            is( $m->redundant, 1, 'Message is redundant' );
            $m->process($empty_handler);
        }

        # test "acking a redundant message should increment the ack_count key" do
        {
            my $header = Test::Beetle->header_with_params( redundant => 1 );
            my $m = Beetle::Message->new(
                body   => 'foo',
                header => $header,
                queue  => "somequeue",
                store  => $store,
            );
            is( $store->get( $m->msg_id => 'ack_count' ), undef, 'The key ack_count is undef in store' );
            $m->process($empty_handler);
            is( $m->redundant, 1, 'Message is redundant' );
            is( $store->get( $m->msg_id => 'ack_count' ), 1, 'The key ack_count is 1 after processing the message' );
        }

        # test "acking a redundant message twice should remove the ack_count key" do
        {
            my $header = Test::Beetle->header_with_params( redundant => 1 );
            my $m = Beetle::Message->new(
                body   => 'foo',
                header => $header,
                queue  => "somequeue",
                store  => $store,
            );
            is( $store->get( $m->msg_id => 'ack_count' ), undef, 'The key ack_count is undef in store' );
            $m->process($empty_handler);
            $m->process($empty_handler);
            is( $m->redundant, 1, 'Message is redundant' );
            is( $store->exists( $m->msg_id, 'ack_count' ),
                0, 'The key ack_count does not exist in store after processing twice' );
        }

        {
            my $header = Test::Beetle->header_with_params();
            my $m      = Beetle::Message->new(
                body   => 'foo',
                header => $header,
                queue  => "queue",
                store  => $store,
            );
            is( $m->aquire_mutex, 1, 'Mutex aquiring #1' );
            is( $m->aquire_mutex, 0, 'Mutex aquiring #2' );
        }

        {
            my $override = Sub::Override->new( 'Beetle::Message::ack' => sub { } );
            my $header   = Test::Beetle->header_with_params();
            my $m        = Beetle::Message->new(
                body       => 'foo',
                header     => $header,
                queue      => "queue",
                store      => $store,
                attempts   => 10,
                exceptions => 5,
            );

            is( $m->attempts_limit,   10, 'attempts limit set correctly' );
            is( $m->exceptions_limit, 5,  'exceptions limit set correctly' );

            is( $m->exceptions_limit_reached, 0, 'exceptions limit not reached yet' );
            is( $m->attempts_limit_reached,   0, 'attempts limit not reached yet' );

            ok( $m->increment_exception_count, 'incr exception count 4 times' ) for 1 .. 4;

            is( $m->_handler_failed, undef, 'No limit reached yet' );
            is( $m->_handler_failed, $EXCEPTIONSLIMITREACHED, 'Exceptions limit reached' );

            ok( $m->increment_execution_attempts, 'incr execution attempts 10 times' ) for 1 .. 10;

            is( $m->_handler_failed, $ATTEMPTSLIMITREACHED, 'Attempts limit reached' );

            is( $m->exceptions_limit_reached, 1, 'exceptions limit reached' );
            is( $m->attempts_limit_reached,   1, 'attempts limit reached' );
        }

        {
            my $header = Test::Beetle->header_with_params();
            my $m      = Beetle::Message->new(
                body    => 'foo',
                header  => $header,
                queue   => "queue",
                store   => $store,
                timeout => 1,
            );
            my $o1 = Sub::Override->new( 'Beetle::Message::now' => sub { return 42; } );

            ok( $m->set_timeout, 'Call to set_timeout works' );
            is( $m->is_timed_out, 0, 'Message is not yet timed out' );

            # Let the time machine run...
            my $o2 = Sub::Override->new( 'Beetle::Message::now' => sub { return 48; } );

            is( $m->is_timed_out, 1, 'Message is timed out now' );
        }

        {
            my $override = Sub::Override->new( 'Beetle::Message::ack' => sub { } );
            my $header   = Test::Beetle->header_with_params();
            my $m        = Beetle::Message->new(
                body             => 'foo',
                header           => $header,
                queue            => "queue",
                store            => $store,
                exceptions_limit => 6,
                attempts_limit   => 5,
                delay            => 0,
            );

            is( $m->attempts_limit,   7, 'attempts limit set correctly to exceptions limit + 1' );
            is( $m->exceptions_limit, 6, 'attempts limit set correctly' );

            my @rc = ();
            for ( 1 .. 7 ) {
                push @rc, $m->_process_internal( sub { die "foo"; } );
            }

            is_deeply(
                \@rc,
                [
                    'RC::HandlerCrash', 'RC::HandlerCrash', 'RC::HandlerCrash', 'RC::HandlerCrash',
                    'RC::HandlerCrash', 'RC::HandlerCrash', 'RC::AttemptsLimitReached'
                ],
                'Got 6 times RC::HandlerCrash + 1 time RC::AttemptsLimitReached'
            );
        }

        {
            my $override = Sub::Override->new( 'Beetle::Message::ack' => sub { } );
            my $header   = Test::Beetle->header_with_params();
            my $m        = Beetle::Message->new(
                body             => 'foo',
                header           => $header,
                queue            => "queue",
                store            => $store,
                attempts_limit   => 7,
                exceptions_limit => 5,
                delay            => 0,
            );

            is( $m->attempts_limit,   7, 'attempts limit set correctly' );
            is( $m->exceptions_limit, 5, 'exceptions limit set correctly' );

            my @rc = ();
            push @rc, $m->_process_internal( sub { die "foo"; } ) for 1 .. 6;

            is_deeply(
                \@rc,
                [
                    'RC::HandlerCrash', 'RC::HandlerCrash',
                    'RC::HandlerCrash', 'RC::HandlerCrash',
                    'RC::HandlerCrash', 'RC::ExceptionsLimitReached'
                ],
                'Got 5 times RC::HandlerCrash + 1 time RC::ExceptionsLimitReached'
            );
        }

        {
            my $header = Test::Beetle->header_with_params();
            my $m      = Beetle::Message->new(
                body     => 'foo',
                header   => $header,
                queue    => "somequeue",
                store    => $store,
                attempts => 2,
            );

            my @callstack = ();

            my $o1 = Sub::Override->new(
                'Beetle::Handler::call' => sub {
                    push @callstack, 'Beetle::Handler::call';
                }
            );

            my $o2 = Sub::Override->new(
                'Beetle::Message::ack' => sub {
                    push @callstack, 'Beetle::Message::ack';
                }
            );

            is( $m->attempts_limit_reached, 0, 'Attempts limit not yet reached' );
            is( $m->process( sub { } ), $OK, 'Return value is correct' );
            is_deeply(
                \@callstack,
                [qw(Beetle::Handler::call Beetle::Message::ack)],
                'processing a fresh message sucessfully should first run the handler and then ack it'
            );
        }

        {
            my $header = Test::Beetle->header_with_params( redundant => 1 );
            my $m = Beetle::Message->new(
                body    => 'foo',
                header  => $header,
                queue   => "somequeue",
                store   => $store,
                timeout => 10,
            );

            my @callstack = ();

            my $o1 = Sub::Override->new(
                'Beetle::Handler::call' => sub {
                    push @callstack, 'Beetle::Handler::call';
                }
            );

            my $o2 = Sub::Override->new(
                'Beetle::Message::ack' => sub {
                    my ($self) = @_;
                    $self->store->incr( $self->msg_id => 'ack_count' );
                    push @callstack, 'Beetle::Message::ack';
                }
            );

            my $o3 = Sub::Override->new(
                'Beetle::Message::completed' => sub {
                    my ($self) = @_;
                    $self->store->set( $self->msg_id => status => 'completed' );
                    push @callstack, 'Beetle::Message::completed';
                }
            );

            is( $m->attempts_limit_reached, 0, 'Attempts limit not yet reached' );
            is( $m->redundant,              1, 'Message is redundant' );
            is( $m->process( sub { } ), $OK, 'Return value is correct' );
            is_deeply(
                \@callstack,
                [
                    qw(Beetle::Handler::call
                      Beetle::Message::completed
                      Beetle::Message::ack)
                ],
                'after processing a redundant fresh message successfully the ack'
                  . ' count should be 1 and the status should be completed'
            );
            is( $store->get( $m->msg_id => 'ack_count' ), 1,           'ack_count is correct' );
            is( $store->get( $m->msg_id => 'status' ),    'completed', 'status is correct' );
        }

        {
            my $header = Test::Beetle->header_with_params();
            my $m      = Beetle::Message->new(
                body     => 'foo',
                header   => $header,
                queue    => "somequeue",
                store    => $store,
                attempts => 1,
            );

            my @callstack = ();

            my $o1 = Sub::Override->new(
                'Beetle::Handler::call' => sub {
                    push @callstack, 'Beetle::Handler::call';
                }
            );

            my $o2 = Sub::Override->new(
                'Beetle::Message::ack' => sub {
                    push @callstack, 'Beetle::Message::ack';
                }
            );

            is( $m->process( sub { } ), $OK, 'Return value is correct' );
            is_deeply(
                \@callstack,
                [qw(Beetle::Message::ack Beetle::Handler::call)],
                'when processing a simple message, ack should precede calling the handler'
            );
        }

        {
            my $header = Test::Beetle->header_with_params();
            my $m      = Beetle::Message->new(
                body     => 'foo',
                header   => $header,
                queue    => "somequeue",
                store    => $store,
                attempts => 1,
            );

            my @callstack = ();

            my $o1 = Sub::Override->new(
                'Beetle::Message::ack' => sub {
                    push @callstack, 'Beetle::Message::ack';
                }
            );

            my $o2 = Sub::Override->new(
                'Beetle::Handler::process_exception' => sub {
                    push @callstack, 'Beetle::Handler::process_exception';
                }
            );

            my $o3 = Sub::Override->new(
                'Beetle::Handler::process_failure' => sub {
                    push @callstack, 'Beetle::Handler::process_failure';
                }
            );

            is( $m->process( sub { die "blah"; } ), $ATTEMPTSLIMITREACHED, 'Return value is correct' );
            is_deeply(
                \@callstack,
                [
                    qw(
                      Beetle::Message::ack
                      Beetle::Handler::process_exception
                      Beetle::Handler::process_failure
                      )
                ],
                'when processing a simple message, RC::AttemptsLimitReached should be returned if the handler crashes'
            );
        }

        # test "a message should not be acked if the handler crashes and the exception limit has not been reached" do
        {
            my $header = Test::Beetle->header_with_params();
            my $m      = Beetle::Message->new(
                body       => 'foo',
                header     => $header,
                queue      => "somequeue",
                store      => $store,
                delay      => 42,
                exceptions => 1,
                timeout    => 10,
            );

            my $o1 = Sub::Override->new( 'Beetle::Message::now' => sub { return 10; } );
            my $o2 = Sub::Override->new(
                'Beetle::Message::completed' => sub {
                    fail('Beetle::Message::completed may not be called');
                }
            );
            my $o3 = Sub::Override->new(
                'Beetle::Message::ack' => sub {
                    fail('Beetle::Message::ack may not be called');
                }
            );

            is( $m->attempts_limit_reached,   0, 'attempts limit is not reached yet' );
            is( $m->exceptions_limit_reached, 0, 'exceptions limit is not reached yet' );
            is( $m->is_timed_out,             0, 'message is not timed out yet' );

            is( $m->process( sub { die "blah"; } ), $HANDLERCRASH, 'Return value is correct' );
            is( $m->is_completed, 0, 'message is still incomplete' );

            is( $store->get( $m->msg_id => 'exceptions' ), 1,  'exceptions count is correct' );
            is( $store->get( $m->msg_id => 'timeout' ),    0,  'timeout was reset' );
            is( $store->get( $m->msg_id => 'delay' ),      52, 'delay has been raised' );
        }

# test "a message should delete the mutex before resetting the timer if attempts and exception limits havn't been reached" do
        {
            my $header = Test::Beetle->header_with_params();
            my $m      = Beetle::Message->new(
                body       => 'foo',
                header     => $header,
                queue      => "somequeue",
                store      => $store,
                delay      => 42,
                exceptions => 1,
                timeout    => 10,
            );

            my $delete_mutex = 0;

            my $o1 = Sub::Override->new( 'Beetle::Message::now' => sub { return 9; } );
            my $o2 = Sub::Override->new(
                'Beetle::Message::completed' => sub {
                    fail('Beetle::Message::completed may not be called');
                }
            );
            my $o3 = Sub::Override->new(
                'Beetle::Message::ack' => sub {
                    fail('Beetle::Message::ack may not be called');
                }
            );
            my $o4 = Sub::Override->new(
                'Beetle::Message::delete_mutex' => sub {
                    $delete_mutex++;
                }
            );

            is( $m->attempts_limit_reached,   0, 'attempts limit is not reached yet' );
            is( $m->exceptions_limit_reached, 0, 'exceptions limit is not reached yet' );
            is( $m->is_timed_out,             0, 'message is not timed out yet' );

            is( $store->get( $m->msg_id => 'mutext' ), undef, 'mutex is not set' );

            is( $m->process( sub { die "blah"; } ), $HANDLERCRASH, 'Return value is correct' );
            is( $delete_mutex, 1, 'delete_mutex got called' );
        }

        # test "a message should be acked if the handler crashes and the exception limit has been reached" do
        {
            my $header = Test::Beetle->header_with_params();
            my $m      = Beetle::Message->new(
                body     => 'foo',
                header   => $header,
                queue    => "somequeue",
                store    => $store,
                attempts => 2,
                timeout  => 10,
            );

            my $ack = 0;

            my $o1 = Sub::Override->new( 'Beetle::Message::now' => sub { return 9; } );
            my $o2 = Sub::Override->new(
                'Beetle::Message::completed' => sub {
                    fail('Beetle::Message::completed may not be called');
                }
            );
            my $o3 = Sub::Override->new(
                'Beetle::Message::ack' => sub {
                    $ack++;
                }
            );

            is( $m->attempts_limit_reached,   0, 'attempts limit is not reached yet' );
            is( $m->exceptions_limit_reached, 0, 'exceptions limit is not reached yet' );
            is( $m->is_timed_out,             0, 'message is not timed out yet' );
            is( $m->simple,                   0, 'message is not simple' );

            is( $m->process( sub { die "blah"; } ), $EXCEPTIONSLIMITREACHED, 'Return value is correct' );
            is( $ack, 1, 'ack got called' );
        }

        # test "a message should be acked if the handler crashes and the attempts limit has been reached" do
        {
            my $header = Test::Beetle->header_with_params();
            my $m      = Beetle::Message->new(
                body     => 'foo',
                header   => $header,
                queue    => "somequeue",
                store    => $store,
                attempts => 2,
                timeout  => 10,
            );
            $m->increment_execution_attempts;

            my $ack = 0;

            my $o1 = Sub::Override->new( 'Beetle::Message::now' => sub { return 9; } );
            my $o2 = Sub::Override->new(
                'Beetle::Message::completed' => sub {
                    fail('Beetle::Message::completed may not be called');
                }
            );
            my $o3 = Sub::Override->new(
                'Beetle::Message::ack' => sub {
                    $ack++;
                }
            );

            is( $m->attempts_limit_reached,   0, 'attempts limit is not reached yet' );
            is( $m->exceptions_limit_reached, 0, 'exceptions limit is not reached yet' );
            is( $m->is_timed_out,             0, 'message is not timed out yet' );
            is( $m->simple,                   0, 'message is not simple' );

            is( $m->process( sub { die "blah"; } ), $ATTEMPTSLIMITREACHED, 'Return value is correct' );
            is( $ack, 1, 'ack got called' );
        }

        # test "a completed existing message should be just acked and not run the handler" do
        {
            my $header = Test::Beetle->header_with_params();
            my $m      = Beetle::Message->new(
                body     => 'foo',
                header   => $header,
                queue    => "somequeue",
                store    => $store,
                attempts => 2,
            );

            my $ack = 0;

            my $o1 = Sub::Override->new(
                'Beetle::Handler::call' => sub {
                    fail('Beetle::Handler::call may not be called');
                }
            );

            my $o2 = Sub::Override->new(
                'Beetle::Message::ack' => sub {
                    $ack++;
                }
            );

            is( $m->key_exists, 0, 'keys do not exist yet' );
            ok( $m->completed, 'set message to completed' );
            is( $m->is_completed, 1, 'message is completed' );

            is( $m->process( sub { } ), $OK, 'Return value is correct' );
        }

        # test "an incomplete, delayed existing message should be processed later" do
        {
            my $header = Test::Beetle->header_with_params();
            my $m      = Beetle::Message->new(
                body     => 'foo',
                header   => $header,
                queue    => "somequeue",
                store    => $store,
                attempts => 2,
            );

            my $o1 = Sub::Override->new(
                'Beetle::Handler::call' => sub {
                    fail('Beetle::Handler::call may not be called');
                }
            );

            my $o2 = Sub::Override->new(
                'Beetle::Message::ack' => sub {
                    fail('Beetle::Message::ack may not be called');
                }
            );

            is( $m->key_exists,   0, 'keys do not exist yet' );
            is( $m->is_completed, 0, 'message is not completed' );
            ok( $m->set_delay, 'set_delay call ok' );
            is( $m->delayed, 1, 'message is delayed' );

            is( $m->process( sub { } ), $DELAYED, 'Return value is correct' );
            is( $m->delayed,      1, 'message is delayed' );
            is( $m->is_completed, 0, 'message is not completed' );
        }

        # test "an incomplete, undelayed, not yet timed out, existing message should be processed later" do
        {
            my $header = Test::Beetle->header_with_params();
            my $m      = Beetle::Message->new(
                body     => 'foo',
                header   => $header,
                queue    => "somequeue",
                store    => $store,
                attempts => 2,
                timeout  => 10,
            );

            my $o1 = Sub::Override->new(
                'Beetle::Handler::call' => sub {
                    fail('Beetle::Handler::call may not be called');
                }
            );

            my $o2 = Sub::Override->new(
                'Beetle::Message::ack' => sub {
                    fail('Beetle::Message::ack may not be called');
                }
            );

            is( $m->key_exists,   0, 'keys do not exist yet' );
            is( $m->is_completed, 0, 'message is not completed' );
            is( $m->delayed,      0, 'message is not delayed' );
            ok( $m->set_timeout, 'set_timeout call ok' );
            is( $m->is_timed_out, 0, 'message is not yet timed out' );
            is( $m->process( sub { } ), $HANDLERNOTYETTIMEDOUT, 'Return value is correct' );
            is( $m->delayed,      0, 'message is not delayed' );
            is( $m->is_completed, 0, 'message is not completed' );
            is( $m->is_timed_out, 0, 'message is not yet timed out' );
        }

        # test "an incomplete, undelayed, not yet timed out, existing message which has
        # reached the handler execution attempts limit should be acked and not run the handler" do
        {
            my $header = Test::Beetle->header_with_params();
            my $m      = Beetle::Message->new(
                body     => 'foo',
                header   => $header,
                queue    => "somequeue",
                store    => $store,
                attempts => 2,
            );

            my $o1 = Sub::Override->new(
                'Beetle::Handler::call' => sub {
                    fail('Beetle::Handler::call may not be called');
                }
            );

            $m->increment_execution_attempts;

            is( $m->key_exists,   0, 'keys do not exist yet' );
            is( $m->is_completed, 0, 'message is not completed' );
            is( $m->delayed,      0, 'message is not delayed' );

            ok( $m->reset_timeout, 'reset_timeout call ok' );
            is( $m->is_timed_out, 1, 'message is timed out' );

            is( $m->attempts_limit_reached, 0, 'attempts_limit_reached not yet reached' );
            $m->increment_execution_attempts for 1 .. $m->attempts_limit;
            is( $m->attempts_limit_reached, 1, 'attempts_limit_reached has been reached now' );

            is( $m->process( sub { } ), $ATTEMPTSLIMITREACHED, 'Return value is correct' );
        }

        # test "an incomplete, undelayed, timed out, existing message which has reached the
        # exceptions limit should be acked and not run the handler" do
        {
            my $header = Test::Beetle->header_with_params();
            my $m      = Beetle::Message->new(
                body     => 'foo',
                header   => $header,
                queue    => "somequeue",
                store    => $store,
                attempts => 2,
            );

            my $o1 = Sub::Override->new(
                'Beetle::Handler::call' => sub {
                    fail('Beetle::Handler::call may not be called');
                }
            );

            is( $m->key_exists,   0, 'keys do not exist yet' );
            is( $m->is_completed, 0, 'message is not completed' );
            is( $m->delayed,      0, 'message is not delayed' );

            ok( $m->reset_timeout, 'reset_timeout call ok' );
            is( $m->is_timed_out, 1, 'message is timed out' );

            is( $m->attempts_limit_reached, 0, 'attempts_limit_reached not yet reached' );
            ok( $m->increment_exception_count, 'call increment_exception_count ok' );
            is( $m->exceptions_limit_reached, 1, 'exceptions_limit_reached has been reached' );

            is( $m->process( sub { } ), $EXCEPTIONSLIMITREACHED, 'Return value is correct' );
        }

        # test "an incomplete, undelayed, timed out, existing message should
        # be processed again if the mutex can be aquired" do
        {
            my $header = Test::Beetle->header_with_params( redundant => 1 );
            my $m = Beetle::Message->new(
                body   => 'foo',
                header => $header,
                queue  => "somequeue",
                store  => $store,
            );

            my @callstack = ();

            my $o1 = Sub::Override->new(
                'Beetle::Message::set_timeout' => sub {
                    push @callstack, 'Beetle::Message::set_timeout';
                }
            );

            my $o2 = Sub::Override->new(
                'Beetle::Message::ack' => sub {
                    push @callstack, 'Beetle::Message::ack';
                }
            );

            my $o3 = Sub::Override->new(
                'Beetle::Handler::call' => sub {
                    push @callstack, 'Beetle::Handler::call';
                }
            );

            is( $m->key_exists,   0, 'keys do not exist yet' );
            is( $m->is_completed, 0, 'message is not completed' );
            is( $m->delayed,      0, 'message is not delayed' );

            ok( $m->reset_timeout, 'reset_timeout call ok' );
            is( $m->is_timed_out, 1, 'message is timed out' );

            is( $m->attempts_limit_reached,   0, 'attempts_limit_reached not yet reached' );
            is( $m->exceptions_limit_reached, 0, 'exceptions_limit_reached not yet reached' );

            is( $m->process( sub { } ), $OK, 'Return value is correct' );
            is( $m->is_completed, 1, 'message is completed' );
            is_deeply(
                \@callstack,
                [
                    qw(Beetle::Message::set_timeout
                      Beetle::Handler::call
                      Beetle::Message::ack)
                ],
                'callstack is correct'
            );
        }

        # test "an incomplete, undelayed, timed out, existing message should not be processed
        # again if the mutex cannot be aquired" do
        {
            my $header = Test::Beetle->header_with_params( redundant => 1 );
            my $m = Beetle::Message->new(
                body   => 'foo',
                header => $header,
                queue  => "somequeue",
                store  => $store,
            );

            my $o1 = Sub::Override->new(
                'Beetle::Handler::call' => sub {
                    fail('Beetle::Handler::call may not be called');
                }
            );

            my $o2 = Sub::Override->new(
                'Beetle::Message::ack' => sub {
                    fail('Beetle::Message::ack may not be called');
                }
            );

            is( $m->key_exists,   0, 'keys do not exist yet' );
            is( $m->is_completed, 0, 'message is not completed' );
            is( $m->delayed,      0, 'message is not delayed' );

            ok( $m->reset_timeout, 'reset_timeout call ok' );
            is( $m->is_timed_out, 1, 'message is timed out' );

            is( $m->attempts_limit_reached,   0, 'attempts_limit_reached not yet reached' );
            is( $m->exceptions_limit_reached, 0, 'exceptions_limit_reached not yet reached' );

            ok( $m->aquire_mutex, 'aquire_mutex call ok' );
            is( $store->exists( $m->msg_id => 'mutex' ), 1, 'mutex is set' );

            is( $m->process( sub { } ), $MUTEXLOCKED, 'Return value is correct' );

            is( $store->exists( $m->msg_id => 'mutex' ), 0, 'mutex is not set' );
        }

        # test "processing a message with a crashing processor calls the processors exception
        # handler and returns an internal error" do
        {
            my $header = Test::Beetle->header_with_params();
            my $m      = Beetle::Message->new(
                body       => 'foo',
                header     => $header,
                queue      => "somequeue",
                store      => $store,
                exceptions => 1,
            );

            my $o1 = Sub::Override->new(
                'Beetle::Handler::process_failure' => sub {
                    fail('Beetle::Handler::process_failure may not be called');
                }
            );

            my @callstack = ();

            my $handler = Beetle::Handler->create(
                sub {
                    push @callstack, 'handler';
                    die "blah";
                },
                {
                    errback => sub {
                        push @callstack, 'errback';
                    },
                },
            );

            my $result = $m->process($handler);

            is( $result, $HANDLERCRASH, 'Return value is correct' );
            is( grep( $result eq $_, @RECOVER ), 1, 'Return value is of correct type' );
            is( grep( $result eq $_, @FAILURE ), 0, 'Return value is of correct type' );
            is_deeply( \@callstack, [qw(handler errback)], 'callstack is correct' );
        }

        # test "processing a message with a crashing processor calls the processors exception handler
        # and failure handler if the attempts limit has been reached" do
        {
            my $header = Test::Beetle->header_with_params();
            my $m      = Beetle::Message->new(
                body     => 'foo',
                header   => $header,
                queue    => "somequeue",
                store    => $store,
                attempts => 2,
            );

            ok( $m->increment_execution_attempts, 'increment_execution_attempts call ok' );

            my @callstack = ();

            my $handler = Beetle::Handler->create(
                sub {
                    push @callstack, 'handler';
                    die "blah";
                },
                {
                    errback => sub {
                        push @callstack, 'errback';
                    },
                    failback => sub {
                        push @callstack, 'failback';
                    },
                },
            );

            my $result = $m->process($handler);

            is( $result, $ATTEMPTSLIMITREACHED, 'Return value is correct' );
            is( grep( $result eq $_, @RECOVER ), 0, 'Return value is of correct type' );
            is( grep( $result eq $_, @FAILURE ), 1, 'Return value is of correct type' );
            is_deeply( \@callstack, [qw(handler errback failback)], 'callstack is correct' );
        }

        # test "processing a message with a crashing processor calls the processors exception handler
        # and failure handler if the exceptions limit has been reached" do
        {
            my $header = Test::Beetle->header_with_params();
            my $m      = Beetle::Message->new(
                body     => 'foo',
                header   => $header,
                queue    => "somequeue",
                store    => $store,
                attempts => 2,
            );

            my @callstack = ();

            my $handler = Beetle::Handler->create(
                sub {
                    push @callstack, 'handler';
                    die "blah";
                },
                {
                    errback => sub {
                        push @callstack, 'errback';
                    },
                    failback => sub {
                        push @callstack, 'failback';
                    },
                },
            );

            my $result = $m->process($handler);

            is( $result, $EXCEPTIONSLIMITREACHED, 'Return value is correct' );
            is( grep( $result eq $_, @RECOVER ), 0, 'Return value is of correct type' );
            is( grep( $result eq $_, @FAILURE ), 1, 'Return value is of correct type' );
            is_deeply( \@callstack, [qw(handler errback failback)], 'callstack is correct' );
        }

        # test "completed! should store the status 'complete' in the database" do
        {
            my $header = Test::Beetle->header_with_params();
            my $m      = Beetle::Message->new(
                body   => 'foo',
                header => $header,
                queue  => "somequeue",
                store  => $store,
            );
            is( $m->is_completed, 0, 'message is not completed' );
            ok( $m->completed, 'completed call ok' );
            is( $m->is_completed, 1, 'message is completed' );
            is( $store->get( $m->msg_id => 'status' ), 'completed', 'status in db is correct' );
        }

        # test "set_delay! should store the current time plus the number of delayed seconds in the database" do
        {
            my $o1     = Sub::Override->new( 'Beetle::Message::now' => sub { return 1; } );
            my $header = Test::Beetle->header_with_params();
            my $m      = Beetle::Message->new(
                body   => 'foo',
                header => $header,
                queue  => "somequeue",
                store  => $store,
                delay  => 1,
            );
            ok( $m->set_delay, 'set_delay call ok' );
            is( $store->get( $m->msg_id => 'delay' ), 2, 'delay is set to 2 in store' );
            my $o2 = Sub::Override->new( 'Beetle::Message::now' => sub { return 2; } );
            is( $m->delayed, 0, 'message is not delayed' );
            my $o3 = Sub::Override->new( 'Beetle::Message::now' => sub { return 0; } );
            is( $m->delayed, 1, 'message is delayed' );
        }

        # test "set_delay! should use the default delay if the delay hasn't been set on the message instance" do
        {
            my $o1     = Sub::Override->new( 'Beetle::Message::now' => sub { return 0; } );
            my $header = Test::Beetle->header_with_params();
            my $m      = Beetle::Message->new(
                body   => 'foo',
                header => $header,
                queue  => "somequeue",
                store  => $store,
            );
            ok( $m->set_delay, 'set_delay call ok' );
            is(
                $store->get( $m->msg_id => 'delay' ),
                $Beetle::Message::DEFAULT_HANDLER_EXECUTION_ATTEMPTS_DELAY,
                'default delay set'
            );
            my $o2 = Sub::Override->new( 'Beetle::Message::now' => sub { return $m->delay; } );
            is( $m->delayed, 0, 'message is not delayed' );
            my $o3 = Sub::Override->new( 'Beetle::Message::now' => sub { return 0; } );
            is( $m->delayed, 1, 'message is delayed' );
        }

        # test "set_timeout! should store the current time plus the number of timeout seconds in the database" do
        {
            my $o1     = Sub::Override->new( 'Beetle::Message::now' => sub { return 1; } );
            my $header = Test::Beetle->header_with_params();
            my $m      = Beetle::Message->new(
                body    => 'foo',
                header  => $header,
                queue   => "somequeue",
                store   => $store,
                timeout => 1,
            );
            ok( $m->set_timeout, 'set_timeout call ok' );
            is( $store->get( $m->msg_id => 'timeout' ), 2, 'timeout set correctly in store' );
            my $o2 = Sub::Override->new( 'Beetle::Message::now' => sub { return 2; } );
            is( $m->is_timed_out, 0, 'message is not timed out yet' );
            my $o3 = Sub::Override->new( 'Beetle::Message::now' => sub { return 3; } );
            is( $m->is_timed_out, 1, 'message is timed out now' );
        }

        # test "set_timeout! should use the default timeout if the timeout hasn't been set on the message instance" do
        {
            my $o1     = Sub::Override->new( 'Beetle::Message::now' => sub { return 0; } );
            my $header = Test::Beetle->header_with_params();
            my $m      = Beetle::Message->new(
                body   => 'foo',
                header => $header,
                queue  => "somequeue",
                store  => $store,
            );
            ok( $m->set_timeout, 'set_timeout call ok' );
            is(
                $store->get( $m->msg_id => 'timeout' ),
                $Beetle::Message::DEFAULT_HANDLER_TIMEOUT,
                'timeout set correctly in store'
            );
            my $o2 = Sub::Override->new( 'Beetle::Message::now' => sub { return $m->timeout; } );
            is( $m->is_timed_out, 0, 'message is not timed out' );
            my $o3 = Sub::Override->new(
                'Beetle::Message::now' => sub { return $Beetle::Message::DEFAULT_HANDLER_TIMEOUT + 1; } );
            is( $m->is_timed_out, 1, 'message is timed out' );
        }

        # test "incrementing execution attempts should increment by 1" do
        {
            my $header = Test::Beetle->header_with_params();
            my $m      = Beetle::Message->new(
                body   => 'foo',
                header => $header,
                queue  => "somequeue",
                store  => $store,
            );
            is( $m->increment_execution_attempts, 1, 'incrementing execution attempts should increment by 1' );
            is( $m->increment_execution_attempts, 2, 'incrementing execution attempts should increment by 1' );
            is( $m->increment_execution_attempts, 3, 'incrementing execution attempts should increment by 1' );
        }

        # test "accessing execution attempts should return the number of execution attempts made so far" do
        {
            my $header = Test::Beetle->header_with_params();
            my $m      = Beetle::Message->new(
                body   => 'foo',
                header => $header,
                queue  => "somequeue",
                store  => $store,
            );
            is( $m->attempts, 0, 'attempts is 0' );
            ok( $m->increment_execution_attempts, 'increment_execution_attempts call ok' );
            is( $m->attempts, 1, 'attempts is 1' );
            ok( $m->increment_execution_attempts, 'increment_execution_attempts call ok' );
            is( $m->attempts, 2, 'attempts is 2' );
            ok( $m->increment_execution_attempts, 'increment_execution_attempts call ok' );
            is( $m->attempts, 3, 'attempts is 3' );
        }

        # test "attempts limit should be set exception limit + 1 iff the configured attempts limit is
        # equal to or smaller than the exceptions limit" do
        {
            my $header = Test::Beetle->header_with_params();
            {
                my $m = Beetle::Message->new(
                    body       => 'foo',
                    header     => $header,
                    queue      => "somequeue",
                    store      => $store,
                    exceptions => 1,
                );
                is( $m->attempts_limit,   2, 'attempts_limit set correctly' );
                is( $m->exceptions_limit, 1, 'exceptions_limit set correctly' );
            }
            {
                my $m = Beetle::Message->new(
                    body       => 'foo',
                    header     => $header,
                    queue      => "somequeue",
                    store      => $store,
                    exceptions => 2,
                );
                is( $m->attempts_limit,   3, 'attempts_limit set correctly' );
                is( $m->exceptions_limit, 2, 'exceptions_limit set correctly' );
            }
            {
                my $m = Beetle::Message->new(
                    body       => 'foo',
                    header     => $header,
                    queue      => "somequeue",
                    store      => $store,
                    attempts   => 5,
                    exceptions => 2,
                );
                is( $m->attempts_limit,   5, 'attempts_limit set correctly' );
                is( $m->exceptions_limit, 2, 'exceptions_limit set correctly' );
            }
        }

        # test "attempts limit should be reached after incrementing the attempt limit counter 'attempts limit' times" do
        {
            my $header = Test::Beetle->header_with_params();
            my $m      = Beetle::Message->new(
                body     => 'foo',
                header   => $header,
                queue    => "somequeue",
                store    => $store,
                attempts => 2,
            );
            is( $m->attempts_limit_reached, 0, 'attempts_limit not reached yet' );
            ok( $m->increment_execution_attempts, 'increment_execution_attempts call ok' );
            is( $m->attempts_limit_reached, 0, 'attempts_limit not reached yet' );
            ok( $m->increment_execution_attempts, 'increment_execution_attempts call ok' );
            is( $m->attempts_limit_reached, 1, 'attempts_limit not reached yet' );
            ok( $m->increment_execution_attempts, 'increment_execution_attempts call ok' );
            is( $m->attempts_limit_reached, 1, 'attempts_limit not reached yet' );
        }

        # test "incrementing exception counts should increment by 1" do
        {
            my $header = Test::Beetle->header_with_params();
            my $m      = Beetle::Message->new(
                body   => 'foo',
                header => $header,
                queue  => "somequeue",
                store  => $store,
            );
            is( $m->increment_exception_count, 1, 'incrementing exception counts should increment by 1' );
            is( $m->increment_exception_count, 2, 'incrementing exception counts should increment by 1' );
            is( $m->increment_exception_count, 3, 'incrementing exception counts should increment by 1' );
        }

        # test "default exceptions limit should be reached after incrementing the attempt limit counter 1 time" do
        {
            my $header = Test::Beetle->header_with_params();
            my $m      = Beetle::Message->new(
                body   => 'foo',
                header => $header,
                queue  => "somequeue",
                store  => $store,
            );
            is( $m->exceptions_limit_reached, 0, 'exceptions limit not reached yet' );
            ok( $m->increment_exception_count, 'increment_exception_count call ok' );
            is( $m->exceptions_limit_reached, 1, 'exceptions reached' );
        }

        # test "exceptions limit should be reached after incrementing the
        # attempt limit counter 'exceptions limit + 1' times" do
        {
            my $header = Test::Beetle->header_with_params();
            my $m      = Beetle::Message->new(
                body       => 'foo',
                header     => $header,
                queue      => "somequeue",
                store      => $store,
                exceptions => 1,
            );
            is( $m->exceptions_limit_reached, 0, 'exceptions limit not reached yet' );
            ok( $m->increment_exception_count, 'increment_exception_count call ok' );
            is( $m->exceptions_limit_reached, 0, 'exceptions limit not reached yet' );
            ok( $m->increment_exception_count, 'increment_exception_count call ok' );
            is( $m->exceptions_limit_reached, 1, 'exceptions reached' );
            ok( $m->increment_exception_count, 'increment_exception_count call ok' );
            is( $m->exceptions_limit_reached, 1, 'exceptions reached' );
        }

        # test "failure to aquire a mutex should delete it from the database" do
        {
            my $header = Test::Beetle->header_with_params();
            my $m      = Beetle::Message->new(
                body   => 'foo',
                header => $header,
                queue  => "somequeue",
                store  => $store,
            );
            is( $store->exists( $m->msg_id => 'mutex' ), 0, 'mutex not in store' );
            is( $m->aquire_mutex, 1, 'mutex aquired' );
            is( $store->exists( $m->msg_id => 'mutex' ), 1, 'mutex in store' );
            is( $m->aquire_mutex, 0, 'mutex could not be aquired' );
            is( $store->exists( $m->msg_id => 'mutex' ), 0, 'mutex got deleted from store' );
        }

        # test "processing a message catches internal exceptions risen by process_internal and returns an internal error" do
        {
            my $o1     = Sub::Override->new( 'Beetle::Message::_process_internal' => sub { die "blah"; } );
            my $header = Test::Beetle->header_with_params();
            my $m      = Beetle::Message->new(
                body   => 'foo',
                header => $header,
                queue  => "somequeue",
                store  => $store,
            );
            my $result = $m->process(sub {});
            is( $result, $INTERNALERROR, 'Return value is correct' );
        }
    }
);

done_testing;
