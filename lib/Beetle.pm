package Beetle;

use Moose;

our $VERSION = '0.01000';

1;

=head1 NAME

Beetle - High availability AMQP messaging with redundant queues

=head1 SYNOPSIS

=head1 DESCRIPTION

Beetle grew out of a project to improve an existing ActiveMQ based messaging
infrastructure. It offers the following features:

=over 4

=item *  High Availability (by using multiple message broker instances)

=item * Redundancy (by replicating queues)

=item * Simple client API (by encapsulating the publishing/ deduplication logic)

=back

This is the Perl implementation of the Ruby Beetle project.
More information can be found on the L<http://xing.github.com/beetle>.

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
