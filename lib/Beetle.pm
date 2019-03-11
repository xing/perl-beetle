package Beetle;

use Moose;
use namespace::clean -except => 'meta';

our $VERSION = '0.31';

__PACKAGE__->meta->make_immutable;

1;

=head1 NAME

Beetle - High availability AMQP messaging with redundant queues

=head1 SYNOPSIS

=head1 DESCRIPTION

This is the Perl implementation of the Ruby Beetle project. The Perl
implementation is as close as possible to the Ruby one.
More information can be found on L<http://xing.github.com/beetle/>.

Beetle grew out of a project to improve an existing ActiveMQ based messaging
infrastructure. It offers the following features:

=over 4

=item *  High Availability (by using multiple message broker instances)

=item * Redundancy (by replicating queues)

=item * Simple client API (by encapsulating the publishing/ deduplication logic)

=back

The main documentation can be found in L<Beetle::Client> which is the public
interface to the L<Beetle::Subscriber> and the L<Beetle::Publisher>. There
are also some examples in the distribution in C<< examples/ >>.

=head1 TEST COVERAGE

    ----------------------------------- ------ ------ ------ ------ ------ ------
    File                                  stmt   bran   cond    sub   time  total
    ----------------------------------- ------ ------ ------ ------ ------ ------
    blib/lib/Beetle.pm                   100.0    n/a    n/a  100.0    1.0  100.0
    lib/Beetle/Base.pm                   100.0    n/a    n/a  100.0    0.8  100.0
    lib/Beetle/Base/PubSub.pm            100.0  100.0  100.0  100.0    2.2  100.0
    lib/Beetle/Bunny.pm                  100.0  100.0   91.7  100.0    3.1   99.0
    lib/Beetle/Client.pm                 100.0  100.0   79.4  100.0   35.3   95.9
    lib/Beetle/Config.pm                 100.0    n/a    n/a  100.0    0.5  100.0
    lib/Beetle/Constants.pm              100.0    n/a    n/a  100.0    0.0  100.0
    lib/Beetle/DeduplicationStore.pm     100.0   95.0   66.7  100.0   10.4   98.9
    lib/Beetle/Handler.pm                100.0  100.0  100.0  100.0    2.7  100.0
    lib/Beetle/Message.pm                100.0  100.0  100.0  100.0   17.1  100.0
    lib/Beetle/Publisher.pm               99.2   93.3  100.0  100.0   14.1   98.3
    lib/Beetle/Redis.pm                   97.5   88.2   50.0   92.3    1.8   90.1
    lib/Beetle/Subscriber.pm             100.0   94.4  100.0  100.0   10.9   99.3
    Total                                 99.7   96.4   85.3   99.5  100.0   98.1
    ----------------------------------- ------ ------ ------ ------ ------ ------

=head1 AUTHOR

Johannes Plunien E<lt>plu@cpan.orgE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright 2010 XING AG

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

IT COMES WITHOUT WARRANTY OF ANY KIND.

=head1 SEE ALSO

=over 4

=item * L<http://xing.github.com/beetle/>

=back

=head1 REPOSITORY

L<http://github.com/plu/perl-beetle/>

=cut
