package Beetle::Base;

use Moose;
with qw(MooseX::Log::Log4perl);
use Beetle::Config;

has 'config' => (
    default => sub { Beetle::Config->new },
    is      => 'ro',
    isa     => 'Beetle::Config',
);

sub BUILD {
    my ($self) = @_;
    $self->_setup_logger;
}

sub _setup_logger {
    my ($self) = @_;

    Log::Log4perl->easy_init({ file => $self->config->logger });
}

1;
