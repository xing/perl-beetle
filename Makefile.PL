use inc::Module::Install;

name 'Beetle';
all_from 'lib/Beetle.pm';
no_index directory => 'examples';

requires(
    'Data::UUID'            => '1.203',
    'Devel::StackTrace'     => '1.22',
    'Hash::Merge::Simple'   => '0.05',
    'Moose'                 => '0.92',
    'MooseX::Log::Log4perl' => '0.40',
    'MooseX::SimpleConfig'  => 0,
    'Net::RabbitFoot'       => '1.01',
);

test_requires(
    'Test::Exception'  => 0,
    'Test::MockObject' => 0,
    'Test::More'       => 0,
    'Test::TCP'        => 0,
    'Test::TCP::Multi' => 0,
);

tests 't/*.t t/*/*.t';
author_tests 'xt';
readme_from 'lib/Beetle.pm';

if ( -e 'MANIFEST.SKIP' ) {
    system('pod2text lib/Beetle.pm > README');
}

auto_install;
WriteAll;
