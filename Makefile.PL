use inc::Module::Install;

name 'Beetle';
all_from 'lib/Beetle.pm';
no_index directory => 'examples';

requires(
    'AnyEvent::Redis' => '0.09',
    'Data::UUID'      => '1.203',
    'Moose'           => '0.92',
);

test_requires(
    'Test::MockObject' => 0,
    'Test::More'       => 0,
);

my @tests = ('t/*.t');

if ( -e 'MANIFEST.SKIP' ) {
    system('pod2text lib/Beetle.pm > README');
    unshift @tests, 'xt/*.t';
}

tests join ' ', @tests;

auto_install;
WriteAll;
