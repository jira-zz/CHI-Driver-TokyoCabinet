use strict;
use warnings;
use Module::Build;

my $builder = Module::Build->new(
    module_name       => 'CHI::Driver::TokyoCabinet',
    license           => 'perl',
    dist_author       => 'Jiří Pavlovský <jira@getnet.cz>',
    dist_version_from => 'lib/CHI/Driver/TokyoCabinet.pm',
    build_requires    => {
        'Test::More'  => 0,
        'CHI'         => 0,
        'Test::Class' => 0,
    },
    requires => {
        'TokyoCabinet' => 0,
        'CHI' => 0.241,
    },
    create_makefile_pl => 'traditional',
);

$builder->create_build_script();
