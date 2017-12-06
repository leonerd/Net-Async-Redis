requires 'parent', 0;
requires 'curry', 0;
requires 'Future', '>= 0.30';
requires 'Protocol::Redis', 0;
requires 'IO::Async', 0;
requires 'Ryu::Async', 0;
requires 'JSON::MaybeXS', 0;
requires 'List::Util', '>= 1.29';

on 'test' => sub {
	requires 'Test::More', '>= 0.98';
};

