use strict;
use warnings;
use Test::Exception;
use Test::More;

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

done_testing;
