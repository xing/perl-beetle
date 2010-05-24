package Beetle::Base;

use Moose;
with qw(MooseX::Log::Log4perl);

sub BUILD {
    my ($self) = @_;
    $self->_setup_logger;
}

sub _setup_logger {
    my ($self) = @_;

    Log::Log4perl->easy_init();
}

1;
