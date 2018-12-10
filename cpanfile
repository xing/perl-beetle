requires 'Coro'                  => '5.23';
requires 'Data::UUID'            => '1.203';
requires 'Devel::StackTrace'     => '1.22';
requires 'Encode'                => '2.23';
requires 'Hash::Merge::Simple'   => '0.05';
requires 'IO'                    => '1.25';
requires 'Moose'                 => '0.92';
requires 'MooseX::Log::Log4perl' => '0.40';
requires 'MooseX::SimpleConfig'  => '0.07';
requires 'namespace::clean'      => '0.11';
requires 'Net::RabbitFoot'       => '1.02';
requires 'Net::AMQP::RabbitMQ'   => '0.010000';
requires 'Scalar::Util'          => '1.21';
requires 'Sys::SigAction'        => '0.11';
requires 'Redis'                 => '1.926';

on 'test' => sub {
    requires 'Sub::Override'    => 0;
    requires 'Test::Exception'  => 0;
    requires 'Test::MockObject' => 0;
    requires 'Test::More'       => 0;
    requires 'Test::TCP'        => 0;
    requires 'Test::TCP::Multi' => 0;
};

on 'develop' => sub {
    requires 'Module::Install' => 0;
    requires 'Module::Install::CPANfile' => 0;
    requires 'Module::Install::ReadmeFromPod' => 0;
    requires 'Module::Install::AuthorTests' => 0;
};
