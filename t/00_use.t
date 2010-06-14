use Test::More;

BEGIN {
    use_ok('Beetle');
    use_ok('Beetle::Base');
    use_ok('Beetle::Base::PubSub');
    use_ok('Beetle::Bunny');
    use_ok('Beetle::Constants');
    use_ok('Beetle::Client');
    use_ok('Beetle::Config');
    use_ok('Beetle::DeduplicationStore');
    use_ok('Beetle::Handler');
    use_ok('Beetle::Message');
    use_ok('Beetle::Publisher');
    use_ok('Beetle::Redis');
    use_ok('Beetle::Subscriber');
}

diag("Testing Beetle $Beetle::VERSION");

done_testing;
