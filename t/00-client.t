#!/usr/bin/perl
# Copyright 2013 craigslist
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

use 5.006;
use strict;
use warnings;

use CLBlob::Client;
use Test::More tests => 29;

my $config = {};
eval { new CLBlob::Client($config) };
ok($@);

$config->{'clblob'} = {};
eval { new CLBlob::Client($config) };
ok($@);

$config->{'clblob'}->{'client'} = {};
eval { new CLBlob::Client($config) };
ok($@);

$config->{'clblob'}->{'client'}->{'clusters'} = [];
$config->{'clblob'}->{'client'}->{'encode_name'} = 1;
$config->{'clblob'}->{'client'}->{'replica_retry'} = 10;
$config->{'clblob'}->{'client'}->{'replicas'} = {};
$config->{'clblob'}->{'client'}->{'ttl'} = undef;
eval { new CLBlob::Client($config) };
ok($@);

$config->{'clblob'}->{'client'}->{'clusters'} = [[]];
eval { new CLBlob::Client($config) };
ok($@);

$config->{'clblob'}->{'client'}->{'replicas'}->{'test'} = {};
eval { new CLBlob::Client($config) };
ok($@);

$config->{'clblob'}->{'client'}->{'clusters'} = [[{}]];
eval { new CLBlob::Client($config) };
ok($@);

$config->{'clblob'}->{'client'}->{'clusters'} = [[{'write_weight' => 1}]];
eval { new CLBlob::Client($config) };
ok($@);

$config->{'clblob'}->{'client'}->{'clusters'} = [[{'replicas' => []}]];
eval { new CLBlob::Client($config) };
ok($@);

$config->{'clblob'}->{'client'}->{'clusters'} =
    [[{'replicas' => ['000'], 'write_weight' => 1}]];
eval { new CLBlob::Client($config) };
ok($@);

$config->{'clblob'}->{'client'}->{'clusters'} =
    [[{'replicas' => ['000', '000'], 'write_weight' => 1}]];
eval { new CLBlob::Client($config) };
ok($@);

$config->{'clblob'}->{'client'}->{'cluster'} = 2;
$config->{'clblob'}->{'client'}->{'clusters'} =
    [[{'replicas' => ['000'], 'write_weight' => 1}]];
eval { new CLBlob::Client($config) };
ok($@);

$config->{'clblob'}->{'client'}->{'cluster'} = 0;
eval { new CLBlob::Client($config) };
ok($@);

$config->{'clblob'}->{'client'}->{'replicas'}->{'000'} =
    {'ip' => '127.0.0.1', 'port' => 20010, 'read_weight' => 1};
eval { new CLBlob::Client($config) };
ok(!$@);

$config->{'clblob'}->{'client'} = {
    'cluster' => 0,
    'clusters' => [
        [
            {'replicas' => ['000', '001', '002'], 'write_weight' => 1},
            {'replicas' => ['010', '011', '012'], 'write_weight' => 1}
        ],
        [
            {'replicas' => ['100', '101', '102'], 'write_weight' => 1},
            {'replicas' => ['110', '111', '112'], 'write_weight' => 1},
            {'replicas' => ['120', '121', '122'], 'write_weight' => 1}
        ]
    ],
    'encode_name' => 0,
    'replica_retry' => 10,
    'replicas' => {
        '000' => {'ip' => '127.0.0.1', 'port' => 20000, 'read_weight' => 1},
        '001' => {'ip' => '127.0.0.1', 'port' => 20001, 'read_weight' => 1},
        '002' => {'ip' => '127.0.0.1', 'port' => 20002, 'read_weight' => 1},
        '010' => {'ip' => '127.0.0.1', 'port' => 20010, 'read_weight' => 1},
        '011' => {'ip' => '127.0.0.1', 'port' => 20011, 'read_weight' => 1},
        '012' => {'ip' => '127.0.0.1', 'port' => 20012, 'read_weight' => 1},
        '100' => {'ip' => '127.0.0.1', 'port' => 20100, 'read_weight' => 1},
        '101' => {'ip' => '127.0.0.1', 'port' => 20101, 'read_weight' => 1},
        '102' => {'ip' => '127.0.0.1', 'port' => 20102, 'read_weight' => 1},
        '110' => {'ip' => '127.0.0.1', 'port' => 20110, 'read_weight' => 1},
        '111' => {'ip' => '127.0.0.1', 'port' => 20111, 'read_weight' => 1},
        '112' => {'ip' => '127.0.0.1', 'port' => 20112, 'read_weight' => 1},
        '120' => {'ip' => '127.0.0.1', 'port' => 20120, 'read_weight' => 1},
        '121' => {'ip' => '127.0.0.1', 'port' => 20121, 'read_weight' => 1},
        '122' => {'ip' => '127.0.0.1', 'port' => 20122, 'read_weight' => 1}},
    'ttl' => undef};
my $client = new CLBlob::Client($config);
is_deeply($client->buckets('1'), {'0' => 0, '1' => 1});
is_deeply($client->replicas('1'), ['000', '001', '002']);
is($client->name('1'), '1');

$config->{'clblob'}->{'client'}->{'encode_name'} = 1;
$client = new CLBlob::Client($config);
eval { $client->buckets('1', 'encoded' => 1) };
ok($@);
eval { $client->replicas('1', 'encoded' => 1) };
ok($@);
eval { $client->replicas('10100_test1', 'encoded' => 1) };
ok($@);
eval { $client->replicas('001000_test1', 'encoded' => 1) };
ok($@);
eval { $client->replicas('_test1', 'encoded' => 1) };
ok($@);
eval { $client->name('_test1') };
ok($@);

is_deeply($client->buckets('00001_1', 'encoded' => 1), {'0' => 0, '1' => 1});
is($client->name('1'), '00001_1');
is_deeply($client->replicas('00001_1', 'encoded' => 1), ['000', '001', '002']);

is_deeply($client->buckets('00102_2', 'encoded' => 1), {'0' => 1, '1' => 2});
is($client->name('2'), '00102_2');
is_deeply($client->replicas('00102_2', 'encoded' => 1), ['010', '011', '012']);
