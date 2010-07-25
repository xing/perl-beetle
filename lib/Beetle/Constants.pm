package Beetle::Constants;

use strict;
use warnings;
use base qw(Exporter);

our @EXPORT = qw(
  $OK
  $ANCIENT
  $ATTEMPTSLIMITREACHED
  $DECODINGERROR
  $EXCEPTIONSLIMITREACHED
  $DELAYED
  $HANDLERCRASH
  $HANDLERNOTYETTIMEDOUT
  $MUTEXLOCKED
  $INTERNALERROR
  @FAILURE
  @RECOVER
);

our $OK                     = 'RC::OK';
our $ANCIENT                = 'RC::Ancient';
our $ATTEMPTSLIMITREACHED   = 'RC::AttemptsLimitReached';
our $DECODINGERROR          = 'RC::DecodingError';
our $EXCEPTIONSLIMITREACHED = 'RC::ExceptionsLimitReached';
our $DELAYED                = 'RC::Delayed';
our $HANDLERCRASH           = 'RC::HandlerCrash';
our $HANDLERNOTYETTIMEDOUT  = 'RC::HandlerNotYetTimedOut';
our $MUTEXLOCKED            = 'RC::MutexLocked';
our $INTERNALERROR          = 'RC::InternalError';

our @FAILURE = ( $ATTEMPTSLIMITREACHED, $DECODINGERROR, $EXCEPTIONSLIMITREACHED );
our @RECOVER = ( $DELAYED, $HANDLERCRASH, $HANDLERNOTYETTIMEDOUT, $MUTEXLOCKED, $INTERNALERROR );

=head1 NAME

Beetle::Constants - ReturnCodes/Constants

=head1 DESCRIPTION

TODO: <plu> add docs

=head1 AUTHOR

See L<Beetle>.

=head1 COPYRIGHT AND LICENSE

See L<Beetle>.

=cut

1;
