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
use Test::More tests => 9;

my $config->{'clblob'}->{'client'} = {
    'cluster' => 0,
    'clusters' => [
        [
            {'replicas' => ['000', '001', '002'], 'write_weight' => 1},
            {'replicas' => ['010', '011', '012'], 'write_weight' => 1}
        ],
        [
            {'replicas' => ['100', '101', '102'], 'write_weight' => 1},
            {'replicas' => ['110', '111', '112'], 'write_weight' => 1}
        ]
    ],
    'encode_name' => 1,
    'replica_retry' => 10,
    'replicas' => {
        '000' => {'ip' => '127.0.0.1', 'port' => 10000, 'read_weight' => 1},
        '001' => {'ip' => '127.0.0.1', 'port' => 10001, 'read_weight' => 1},
        '002' => {'ip' => '127.0.0.1', 'port' => 10002, 'read_weight' => 1},
        '010' => {'ip' => '127.0.0.1', 'port' => 10010, 'read_weight' => 1},
        '011' => {'ip' => '127.0.0.1', 'port' => 10011, 'read_weight' => 1},
        '012' => {'ip' => '127.0.0.1', 'port' => 10012, 'read_weight' => 1},
        '100' => {'ip' => '127.0.0.1', 'port' => 10100, 'read_weight' => 1},
        '101' => {'ip' => '127.0.0.1', 'port' => 10101, 'read_weight' => 1},
        '102' => {'ip' => '127.0.0.1', 'port' => 10102, 'read_weight' => 1},
        '110' => {'ip' => '127.0.0.1', 'port' => 10110, 'read_weight' => 1},
        '111' => {'ip' => '127.0.0.1', 'port' => 10111, 'read_weight' => 1},
        '112' => {'ip' => '127.0.0.1', 'port' => 10112, 'read_weight' => 1}},
    'ttl' => undef};
my $client = new CLBlob::Client($config);

SKIP: {
    skip "Live tests", 9 unless $ENV{'TEST_LIVE'};
    my $response = $client->put('test', 'test blob data');
    ok(!$response->{'deleted'});
    sleep(1);
    is($client->get($response->{'name'}), 'test blob data');
    $response = $client->get($response->{'name'}, 'response' => 'info');
    ok(!$response->{'deleted'});
    $response = $client->delete($response->{'name'});
    ok($response->{'deleted'});
    $response = $client->buffer('000');
    ok($response->{'queued'});
    $response = $client->list('000');
    is(ref($response), 'ARRAY');
    $response = $client->purge('000');
    ok($response->{'queued'});
    $response = $client->status('000');
    is($response->{'cluster'}, 0);
    $response = $client->sync('000');
    ok($response->{'queued'});
}
