package Beetle::Constants;

use strict;
use warnings;
use base qw(Exporter);

our @EXPORT = qw(
  $OK
  $ANCIENT
  $ATTEMPTSLIMITREACHED
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
our $EXCEPTIONSLIMITREACHED = 'RC::ExceptionsLimitReached';
our $DELAYED                = 'RC::Delayed';
our $HANDLERCRASH           = 'RC::HandlerCrash';
our $HANDLERNOTYETTIMEDOUT  = 'RC::HandlerNotYetTimedOut';
our $MUTEXLOCKED            = 'RC::MutexLocked';
our $INTERNALERROR          = 'RC::InternalError';

our @FAILURE = ( $ATTEMPTSLIMITREACHED, $EXCEPTIONSLIMITREACHED );
our @RECOVER = ( $DELAYED, $HANDLERCRASH, $HANDLERNOTYETTIMEDOUT, $MUTEXLOCKED, $INTERNALERROR );

1;
