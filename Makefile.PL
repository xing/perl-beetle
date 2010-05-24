use inc::Module::Install;

name 'Beetle';
all_from 'lib/Beetle.pm';
no_index directory => 'examples';

requires(
    'Data::UUID'            => '1.203',
    'Moose'                 => '0.92',
    'MooseX::Log::Log4perl' => 0,         # TODO: <plu> add version we have on live
);

test_requires(
    'Test::Exception'  => 0,
    'Test::MockObject' => 0,
    'Test::More'       => 0,
    'Test::TCP'        => 0,
);

my @tests = ('t/*.t');

if ( -e 'MANIFEST.SKIP' ) {
    system('pod2text lib/Beetle.pm > README');
    unshift @tests, 'xt/*.t';
}

tests join ' ', @tests;

auto_install;
WriteAll;
