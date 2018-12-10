use inc::Module::Install;

name 'Beetle';
all_from 'lib/Beetle.pm';
no_index directory => 'examples';

cpanfile;

tests 't/*.t t/*/*.t';
author_tests 'xt';
readme_from 'lib/Beetle.pm';

if ( -e 'MANIFEST.SKIP' ) {
    system('pod2text lib/Beetle.pm > README');
}

auto_install;
WriteAll;
