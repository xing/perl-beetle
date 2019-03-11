package Beetle::Config;

use Moose;
use namespace::clean -except => 'meta';
with qw(MooseX::SimpleConfig);

=head1 NAME

Beetle::Config - Beetle config attributes

=head1 DESCRIPTION

TODO: <plu> add docs

=head1 ATTRIBUTES

=head2 logger

default logfile (default: STDERR)

=cut

has 'logger' => (
    default       => 'STDERR',
    documentation => 'default logfile (default: STDERR)',
    is            => 'rw',
    isa           => 'Str',
    required      => 1,
);

=head2 loglayout

Log4perl log layout (default: [%d] [%p] (%C:%L) %m%n)

=cut

has 'loglayout' => (
    default       => '[%d] [%p] (%C:%L) %m%n',
    documentation => 'Log4perl log layout',
    is            => 'rw',
    isa           => 'Str',
    required      => 1,
);

=head2 loglevel

Log4perl log level (default: INFO)

=cut

has 'loglevel' => (
    default       => 'INFO',
    documentation => 'Log4perl log level',
    is            => 'rw',
    isa           => 'Str',
    required      => 1,
);

=head2 gc_threshold

number of seconds after which keys are removed form the deduplication store (default: 3 days)

=cut

has 'gc_threshold' => (
    default       => 86400 * 3,
    documentation => 'number of seconds after which keys are removed form the deduplication store (default: 3 days)',
    is            => 'rw',
    isa           => 'Int',
    required      => 1,
);

=head2 redis_hosts

the machines where the deduplication store lives (default: localhost:6379)

=cut

has 'redis_hosts' => (
    default       => 'localhost:6379',
    documentation => 'the machines where the deduplication store lives (default: localhost:6379)',
    is            => 'rw',
    isa           => 'Str',
    required      => 1,
);

=head2 redis_db

redis database number to use for the message deduplication store (default: 4)

=cut

has 'redis_db' => (
    default       => 4,
    documentation => 'redis database number to use for the message deduplication store (default: 4)',
    is            => 'rw',
    isa           => 'Int',
    required      => 1,
);

=head2 redis_operation_retries

how often we should retry a redis operation before giving up (default: 180)

=cut

has 'redis_operation_retries' => (
    default       => 180,
    documentation => 'how often we should retry a redis operation before giving up (default: 180)',
    is            => 'rw',
    isa           => 'Int',
    required      => 1,
);

=head2 system_name

dedup redis cluster name to use

=cut

has 'system_name' => (
    default       => 'system',
    documentation => 'dedup redis cluster name to use',
    is            => 'rw',
    isa           => 'Str',
);

=head2 servers

list of amqp servers to use (default: localhost:5672)

=cut

has 'servers' => (
    default       => 'localhost:5672',
    documentation => 'list of amqp servers to use (default: localhost:5672)',
    is            => 'rw',
    isa           => 'Str',
    required      => 1,
);

=head2 additional_subscription_servers

list of additional amqp servers to use for subscribers only

=cut

has 'additional_subscription_servers' => (
    default       => '',
    documentation => 'list of additional amqp servers to use for subscribers only',
    is            => 'rw',
    isa           => 'Str',
    required      => 0,
);

=head2 vhost

the virtual host to use on the AMQP servers (default: /)

=cut

has 'vhost' => (
    default       => '/',
    documentation => 'the virtual host to use on the AMQP servers (default: /)',
    is            => 'rw',
    isa           => 'Str',
    required      => 1,
);

=head2 user

the AMQP user to use when connecting to the AMQP servers (default: guest)

=cut

has 'user' => (
    default       => 'guest',
    documentation => 'the AMQP user to use when connecting to the AMQP servers (default: guest)',
    is            => 'rw',
    isa           => 'Str',
    required      => 1,
);

=head2 password

the password to use when connectiong to the AMQP servers (default: guest)

=cut

has 'password' => (
    default       => 'guest',
    documentation => 'the password to use when connectiong to the AMQP servers (default: guest)',
    is            => 'rw',
    isa           => 'Str',
    required      => 1,
);

=head2 verbose

enable verbose logging, especially the AMQP frames (default: 0)

=cut

has 'verbose' => (
    default       => 0,
    documentation => 'enable verbose logging, especially the AMQP frames (default: 0)',
    is            => 'rw',
    isa           => 'Bool',
    required      => 1,
);

=head2 bunny_class

defaults to Beetle::Bunny

=cut

has 'bunny_class' => (
    default       => 'Beetle::Bunny',
    documentation => 'defaults to Beetle::Bunny',
    is            => 'rw',
    isa           => 'Str',
    required      => 1,
);

=head2 mq_class

defaults to Beetle::AMQP

=cut

has 'mq_class' => (
    default       => 'Beetle::AMQP',
    documentation => 'defaults to Beetle::AMQP',
    is            => 'rw',
    isa           => 'Str',
    required      => 1,
);

__PACKAGE__->meta->make_immutable;

=head1 AUTHOR

See L<Beetle>.

=head1 COPYRIGHT AND LICENSE

See L<Beetle>.

=cut

1;
