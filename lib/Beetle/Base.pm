package Beetle::Base;

use Moose;
use namespace::clean -except => 'meta';
with qw(MooseX::Log::Log4perl);
use Beetle::Config;

=head1 NAME

Beetle::Config - Beetle base class

=head1 DESCRIPTION

TODO: <plu> add docs

=cut

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

__PACKAGE__->meta->make_immutable;

=head1 AUTHOR

See L<Beetle>.

=head1 COPYRIGHT AND LICENSE

See L<Beetle>.

=cut

1;
