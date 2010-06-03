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

around 'BUILDARGS' => sub {
    my $orig  = shift;
    my $class = shift;
    my %args  = @_;

    if ( defined $args{config} ) {
        $args{config} = Beetle::Config->new( %{ delete $args{config} } );
    }

    elsif ( defined $args{configfile} ) {
        $args{config} = Beetle::Config->new_with_config( configfile => delete $args{configfile} );
    }

    return $class->$orig(%args);
};

sub _setup_logger {
    my ($self) = @_;

    Log::Log4perl->easy_init(
        {
            file   => $self->config->logger,
            layout => $self->config->loglayout,
            level  => $self->config->loglevel,
        }
    );
}

1;
