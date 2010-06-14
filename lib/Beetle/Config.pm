package Beetle::Config;

use Moose;
with qw(MooseX::SimpleConfig);

has 'logger' => (
    default       => 'STDERR',
    documentation => 'default logfile (default: STDERR)',
    is            => 'rw',
    isa           => 'Str',
    required      => 1,
);

has 'loglayout' => (
    default       => '[%d] [%p] (%C:%L) %m%n',
    documentation => 'Log4perl log layout',
    is            => 'rw',
    isa           => 'Str',
    required      => 1,
);

has 'loglevel' => (
    default       => 'DEBUG',
    documentation => 'Log4perl log level',
    is            => 'rw',
    isa           => 'Str',
    required      => 1,
);

has 'gc_threshold' => (
    default       => 86400 * 3,
    documentation => 'number of seconds after which keys are removed form the deduplication store (default: 3 days)',
    is            => 'rw',
    isa           => 'Int',
    required      => 1,
);

has 'redis_hosts' => (
    default       => 'localhost:6379',
    documentation => 'the machines where the deduplication store lives (default: localhost:6379)',
    is            => 'rw',
    isa           => 'Str',
    required      => 1,
);

has 'redis_db' => (
    default       => 4,
    documentation => 'redis database number to use for the message deduplication store (default: 4)',
    is            => 'rw',
    isa           => 'Int',
    required      => 1,
);

has 'servers' => (
    default       => 'localhost:5672',
    documentation => 'list of amqp servers to use (default: localhost:5672)',
    is            => 'rw',
    isa           => 'Str',
    required      => 1,
);

has 'vhost' => (
    default       => '/',
    documentation => 'the virtual host to use on the AMQP servers (default: /)',
    is            => 'rw',
    isa           => 'Str',
    required      => 1,
);

has 'user' => (
    default       => 'guest',
    documentation => 'the AMQP user to use when connecting to the AMQP servers (default: guest)',
    is            => 'rw',
    isa           => 'Str',
    required      => 1,
);

has 'password' => (
    default       => 'guest',
    documentation => 'the password to use when connectiong to the AMQP servers (default: guest)',
    is            => 'rw',
    isa           => 'Str',
    required      => 1,
);

has 'verbose' => (
    default       => 0,
    documentation => 'enable verbose logging, especially the AMQP frames (default: 0)',
    is            => 'rw',
    isa           => 'Bool',
    required      => 1,
);

has 'bunny_class' => (
    default       => 'Beetle::Bunny',
    documentation => 'defaults to Beetle::Bunny',
    is            => 'rw',
    isa           => 'Str',
    required      => 1,
);

1;
