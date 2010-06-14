use strict;
use warnings;
use Test::Exception;
use Test::More;

use FindBin qw( $Bin );
use lib ( "$Bin/lib", "$Bin/../lib" );
use Test::Beetle::Handler::FooBar;
use Test::Beetle::Handler::SubFooBar;
use Test::Beetle;

BEGIN {
    use_ok('Beetle::Handler');
    use_ok('Beetle::Message');
}

{
    my $test_var = 0;
    my $handler  = Beetle::Handler->create(
        sub {
            my $message = shift;
            $test_var = $message;
        }
    );
    $handler->call(1);
    is( $test_var, 1, 'should allow using a block as a callback' );
}

{
    my $handler = Beetle::Handler->create('Test::Beetle::Handler::FooBar');
    isa_ok( $handler, 'Test::Beetle::Handler::FooBar', 'Got correct object' );

    my $result = $handler->call('some_message');
    is( $result, 'SOME_MESSAGE', 'should allow using a subclass of a handler as a callback' );
}

{
    my $handler = Beetle::Handler->create( Test::Beetle::Handler::FooBar->new );
    isa_ok( $handler, 'Test::Beetle::Handler::FooBar', 'Got correct object' );

    my $result = $handler->call('some_message');
    is( $result, 'SOME_MESSAGE', 'should allow using an instance of a subclass of handler as a callback' );
}

{
    my $handler = Beetle::Handler->create('Test::Beetle::Handler::FooBar');
    is( $handler->message, undef, 'The message attribute is not set yet' );
    $handler->call('message received');
    is( $handler->message, 'message received', 'should set the instance variable message to the received message' );
}

{
    my $handler = Beetle::Handler->create( sub { } );
    lives_ok { $handler->process; } 'default implementation of process should not crash';
    lives_ok { $handler->error('barfoo'); } 'default implementation of error should not crash';
    lives_ok { $handler->failure('razzmatazz'); } 'default implementation of failure should not crash';
}

{
    my $handler = Beetle::Handler->create('Test::Beetle::Handler::SubFooBar');
    no warnings 'redefine';
    local *Beetle::Handler::error = sub {
        my ( $self, $exception ) = @_;
        return uc $exception . '001';
    };
    my $result = $handler->process_exception('some exception');
    is(
        $result,
        'SOME EXCEPTION001',
        'should call the error method with the exception if no error callback has been given'
    );
}

{
    my $handler = Beetle::Handler->create(
        'Test::Beetle::Handler::SubFooBar',
        {
            errback => sub {
                my ( $self, $exception ) = @_;
                return uc $exception . '002';
            },
        }
    );
    my $result = $handler->process_exception('some exception');
    is( $result, 'SOME EXCEPTION002', 'should call the given error callback with the exception' );
}

{
    my $handler = Beetle::Handler->create('Test::Beetle::Handler::SubFooBar');
    no warnings 'redefine';
    *Beetle::Handler::failure = sub {
        my ( $self, $exception ) = @_;
        return uc $exception . '003';
    };
    my $result = $handler->process_failure('some failure');
    is(
        $result,
        'SOME FAILURE003',
        'should call the failure method with the exception if no failure callback has been given'
    );
}

{
    my $handler = Beetle::Handler->create(
        'Test::Beetle::Handler::SubFooBar',
        {
            failback => sub {
                my ( $self, $exception ) = @_;
                return uc $exception . '004';
            },
        }
    );
    my $result = $handler->process_failure('some failure');
    is( $result, 'SOME FAILURE004', 'should call the given failure callback with the result' );
}

{
    my $handler = Beetle::Handler->create(
        sub {

            # nothing
        },
        {
            errback => sub {
                die "blah";
            },
        }
    );
    lives_ok { $handler->process_exception('test'); } 'should silently rescue exceptions in the process_exception call';
}

{
    my $handler = Beetle::Handler->create(
        sub {

            # nothing
        },
        {
            failback => sub {
                die "blah";
            },
        }
    );
    lives_ok { $handler->process_failure('test'); } 'should silently rescue exceptions in the process_failure call';
}

done_testing;
