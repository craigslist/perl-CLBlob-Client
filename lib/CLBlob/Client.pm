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

package CLBlob::Client;

use 5.006;
use strict;
use warnings;

use Digest::MD5;
use HTTP::Request;
use JSON;
use LWP::UserAgent;
use Math::BaseCalc;

my $BASE62 = new Math::BaseCalc(digits => [0..9, "a".."z", "A".."Z"]);

=head1 NAME

CLBlob::Client - Craigslist blob client module.

=head1 VERSION

Version 0.01

=cut

our $VERSION = '0.01';

=head1 AUTHOR

craigslist, C<< <opensource at craigslist.org> >>

=cut

=head1 SYNOPSIS

This module provides a client API to the craigslist blob service. Regular
blob commands as well as admin commands are supported. The constructor
requires a hash object that contains the config as specified by the blob
service (see the full blob service docs for details).

=head1 SUBROUTINES/METHODS

=head2 new

Create a blob client object.

=cut

sub new {
    my $class = shift;
    my $config = shift;
    my $self = {};
    die 'Invalid config' unless $config->{'clblob'} &&
        $config->{'clblob'}->{'client'};
    $config = $config->{'clblob'}->{'client'};
    for (qw(clusters encode_name replica_retry replicas ttl)) {
        die "$_ not configured" unless exists($config->{$_});
        $self->{$_} = $config->{$_};
    }
    die 'No clusters configured' unless scalar(@{$config->{'clusters'}});
    die 'No replicas configured' unless scalar(keys(%{$config->{'replicas'}}));
    $self->{'replica_info'} = {};
    for my $cluster (0..$#{$self->{'clusters'}}) {
        my $buckets = $self->{'clusters'}->[$cluster];
        die "No buckets configured for cluster: $cluster"
            unless scalar(@$buckets);
        for my $bucket (0..$#$buckets) {
            my $bucket_info = $buckets->[$bucket];
            for my $option (qw(write_weight replicas)) {
                die "No $option for cluster/bucket: $cluster/$bucket"
                    unless exists($bucket_info->{$option});
            }
            for my $replica (@{$bucket_info->{'replicas'}}) {
                die "Replica can only be in one bucket: $replica"
                    if $self->{'replica_info'}->{$replica};
                die "Replica in bucket but not defined: $replica"
                    unless $self->{'replicas'}->{$replica};
                for my $option (qw(ip port read_weight)) {
                    die "No $option for replica: $$replica" unless
                        exists($self->{'replicas'}->{$replica}->{$option});
                }
                $self->{'replica_info'}->{$replica} = {};
            }
        }
    }
    if (defined($config->{'cluster'})) {
        my $cluster_count = scalar(@{$config->{'clusters'}});
        die "Cluster option out of range: $config->{cluster} $cluster_count"
            if $config->{'cluster'} >= $cluster_count;
        $self->{'cluster'} = $config->{'cluster'};
    }
    bless $self, $class;
    return $self;
}

=head2 buckets

Get the hash of buckets for the given name.

=cut

sub buckets {
    my $self = shift;
    my $name = shift;
    my %optional = @_;
    my $event = {
        'name' => $self->_check_name($name),
        'encoded' => $optional{'encoded'} || 0};
    return { %{$self->_buckets($event)} };
}

=head2 delete

Request to eventually delete a specific blob from the blob service at a
specific time. A delete simply marks a blob as being acceptable for purge
after a certain point in time. This does not guarantee the blob will be
removed at this time, it merely provides instruction that the blob should
be removed in the next purge operation.

Delete can be called with either a ttl or a deleted time:

* A ttl is a relative timestamp, implying "expire this blob TTL seconds from
  now. This can be a positive or negative value."
* A deleted time is a specific unix epoch timestamp, implying "expire this
  blob at this unix epoch time."

By default, a delete submits a TTL of 0, meaning "expire this as soon
as possible." Every delete request is tagged with a unique timestamp,
and the most recent one is used. This makes it possible for a delete
request to have no effect if a later stamped request has already completed.

This returns a hash of the current metadata for the blob after the delete
request has been processed.

=cut

sub delete {
    my $self = shift;
    my $name = shift;
    my %optional = @_;
    my $event = {
        'method' => 'DELETE',
        'name' => $self->_check_name($name),
        'deleted' => $self->_make_deleted($optional{'ttl'} || 0,
            $optional{'deleted'}),
        'modified_deleted' => $optional{'modified_deleted'},
        'replicate' => $optional{'replicate'} || 'all',
        'params' => ['deleted', 'modified_deleted', 'replicate']};
    return $self->_forward($event);
}

=head2 get

Get the blob data (if response='data') or blob info (if response='info')
for the given name from the blob service.

=cut

sub get {
    my $self = shift;
    my $name = shift;
    my %optional = @_;
    my $event = {
        'method' => 'GET',
        'name' => $self->_check_name($name),
        'response' => $optional{'response'} || 'data',
        'params' => ['response']};
    $event->{'parse_response'} = 0 if $event->{'response'} eq 'data';
    return $self->_forward($event);
}

=head2 name

Get the blob name that will be used for a given name. This returns an
encoded name when encode_name is true in the config, otherwise it returns
what was given.

=cut

sub name {
    my $self = shift;
    my $name = shift;
    my $event = {
        'name' => $self->_check_name($name),
        'encoded' => 0};
    $self->_encode_name($event) if $self->{'encode_name'};
    return $event->{'name'};
}

=head2 put

Put a blob into the blob service. The ttl and deleted parameters behave the
same as with delete requests, so see the delete method for details. This
returns a hash of the current metadata for the blob after the delete
request has been processed.

=cut

sub put {
    my $self = shift;
    my $name = shift;
    my $data = shift;
    my %optional = @_;
    my $encoded = $optional{'encoded'} || 0;
    my $event = {
        'method' => 'PUT',
        'name' => $self->_check_name($name),
        'data' => $data,
        'modified' => $optional{'modified'},
        'deleted' => $self->_make_deleted($optional{'ttl'},
            $optional{'deleted'}),
        'modified_deleted' => $optional{'modified_deleted'},
        'replicate' => $optional{'replicate'} || 'all',
        'encoded' => $optional{'encoded'} || 0,
        'params' => ['modified', 'deleted', 'modified_deleted', 'replicate',
            'encoded']};
    if (!$event->{'encoded'} && $self->{'encode_name'}) {
        $self->_encode_name($event);
    }
    return $self->_forward($event);
}

=head2 replicas

Get the list of replicas for the given name.

=cut

sub replicas {
    my $self = shift;
    my $name = shift;
    my %optional = @_;
    my $event = {
        'name' => $self->_check_name($name),
        'encoded' => $optional{'encoded'} || 0};
    my @replicas = sort(keys(%{$self->_replicas($event)}));
    return \@replicas;
}

=head2 _check_name

Make sure the name is valid.

=cut

sub _check_name {
    my $self = shift;
    my $name = shift;
    die 'Name cannot be empty' if $name eq '';
    die 'Name cannot cannot start with a _' if substr($name, 0, 1) eq '_';
    return $name;
}

=head2 _make_deleted

Make a deleted time from a ttl if needed.

=cut

sub _make_deleted {
    my $self = shift;
    my $ttl = shift;
    my $deleted = shift;
    if (defined($deleted)) {
        return $deleted;
    }
    if (!defined($ttl)) {
        $ttl = $self->{'ttl'};
        return 0 unless defined($ttl);
    }
    return time() + $ttl;
}

=head2 _encode_name

Make a name encoded with clusters and buckets.

=cut

sub _encode_name {
    my $self = shift;
    my $event = shift;
    my $version = shift || 0;
    my @encoded = ($BASE62->to_base($version));
    my $buckets = $self->_buckets($event);
    for my $cluster (sort(keys(%$buckets))) {
        push(@encoded, sprintf('%02s', $BASE62->to_base($buckets->{$cluster})));
    }
    $event->{'name'} = join('', @encoded) . '_' . $event->{'name'};
    $event->{'encoded'} = 1;
}

=head2 buffer

Queue request to buffer any pending events from the index. The default blob
service configuration is setup to buffer and process replication events at
a moderate pace to not overwhelm machine resources.  This admin command
is a mechanism for allowing rapid and immediate buffering of outstanding
events. A buffer call, from the console, HTTP request, or command-line
spawns a request to queue events and place them into the buffer right away.

=cut

sub buffer {
    my $self = shift;
    my $replica = shift;
    my $event = {
        'method' => 'GET',
        'name' => '_buffer'};
    my $response = $self->_request($replica, $event);
    die "Request failed to $replica: $event->{error}" if $event->{'error'};
    return $response;
}

=head2 list

Get a list of files, or a checksum of the list.

=cut

sub list {
    my $self = shift;
    my $replica = shift;
    my %optional = @_;
    my $event = {
        'method' => 'GET',
        'name' => '_list',
        'params' => ['modified_start', 'modified_stop', 'checksum',
            'checksum_modulo']};
    for (@{$event->{'params'}}) {
        $event->{$_} = $optional{$_};
    }
    my $response = $self->_request($replica, $event);
    die "Request failed to $replica: $event->{error}" if $event->{'error'};
    return $response;
}

=head2 purge

Queue request to delete any blobs that have expired. Each replica will
periodically check the index of blobs and remove those blobs which
have been marked for deletion. The frequency of this automatic purge is
configurable at the client layer.

In addition to this periodic process, a purge request can be sent
directly to a server or client to trigger an immediate purge of the
requested replica.

=cut

sub purge {
    my $self = shift;
    my $replica = shift;
    my $event = {
        'method' => 'GET',
        'name' => '_purge'};
    my $response = $self->_request($replica, $event);
    die "Request failed to $replica: $event->{error}" if $event->{'error'};
    return $response;
}

=head2 status

Get status information for the given replica. This returns a dictionary
object of blob service information as understood by the client that
handles the request. This call, and the resulting data, can be viewed on
the command line or via the HTTP console to provide near-realtime status
of known blob service clusters, buckets, and replicas.

=cut

sub status {
    my $self = shift;
    my $replica = shift;
    my $event = {
        'method' => 'GET',
        'name' => '_status'};
    my $response = $self->_request($replica, $event);
    die "Request failed to $replica: $event->{error}" if $event->{'error'};
    return $response;
}

=head2 sync

Queue request to sync with other peer replicas.

=cut

sub sync {
    my $self = shift;
    my $replica = shift;
    my %optional = @_;
    my $event = {
        'method' => 'GET',
        'name' => '_sync',
        'params' => ['source', 'modified_start', 'modified_stop']};
    for (@{$event->{'params'}}) {
        $event->{$_} = $optional{$_};
    }
    my $response = $self->_request($replica, $event);
    die "Request failed to $replica: $event->{error}" if $event->{'error'};
    return $response;
}

=head2 _forward

Forward a blob event on to a replica that can handle it.

=cut

sub _forward {
    my $self = shift;
    my $event = shift;
    for my $replica (@{$self->_get_best_replica_list($event)}) {
        my $response = $self->_request($replica, $event);
        return $response if defined($response);
    }
    die "Request failed to all replicas: $event->{error}" if $event->{'error'};
    die 'Blob not found on any replicas';
}

=head2 _get_best_replica_list

Get the best set of replicas to try for a request. This puts recently
failed replicas at the end of the list, and randomizes the non-failed
replicas to try and prevent any hot spots.

=cut

sub _get_best_replica_list {
    my $self = shift;
    my $event = shift;
    my $now = time();
    my %best;
    my $total_weight = 0;
    my @failed;
    for my $replica (keys(%{$self->_replicas($event)})) {
        my $last_failed = $self->{'replica_info'}->{$replica}->{'last_failed'}
            || 0;
        if ($last_failed + $self->{'replica_retry'} < $now) {
            my $weight;
            if ($event->{'method'} eq 'GET') {
                $weight = $self->{'replicas'}->{$replica}->{'read_weight'};
            }
            else {
                $weight = 1;
            }
            if ($weight > 0) {
                $best{$replica} = $weight;
                $total_weight += $weight;
            }
        }
        else {
            push(@failed, [$replica, $last_failed]);
        }
    }
    my @weighted_best;
    while (scalar(keys(%best)) > 0) {
        my $number = rand($total_weight);
        my $current = 0;
        while ((my $replica, my $weight) = each(%best)) {
            if ($current <= $number && $number < $current + $weight) {
                push(@weighted_best, $replica);
                $total_weight -= $weight;
                delete $best{$replica};
                last;
            }
            $current += $weight;
        }
    }
    push(@weighted_best, map {$_->[0]} sort {$a->[1] cmp $b->[1]} @failed);
    return \@weighted_best;
}

=head2 _request

Send a HTTP request to the given replica.

=cut

sub _request {
    my $self = shift;
    my $replica = shift;
    my $event = shift;
    my $url = "http://$self->{replicas}->{$replica}->{ip}";
    $url .= ":$self->{replicas}->{$replica}->{port}";
    $url .= $self->_url($event);
    my $request = new HTTP::Request($event->{'method'}, $url);
    if (exists($event->{'data'})) {
        $request->header('content-length' => length($event->{'data'}));
        $request->content($event->{'data'});
    }
    my $agent = LWP::UserAgent->new();
    $agent->timeout($self->{'request_timeout'});
    my $response = $agent->request($request);
    if ($response->is_success) {
        if ((!exists($event->{'parse_response'}) ||
            $event->{'parse_response'}) &&
            $response->header('content-type') eq 'application/json') {
            return decode_json($response->content);
        }
        return $response->content;
    }
    elsif ($response->code() != 404) {
        $event->{'error'} = $response->code() . " " . $response->content;
        $self->{'replica_info'}->{$replica}->{'last_failed'} = time();
    }
    return undef;
}

=head2 _url

Make a URL for this event.

=cut

sub _url {
    my $self = shift;
    my $event = shift;
    my $url = '/' . $event->{'name'};
    return $url unless $event->{'params'};
    my $separator = '?';
    for my $param (@{$event->{'params'}}) {
        if (defined($event->{$param})) {
            $url .= "$separator$param=$event->{$param}";
            $separator = '&';
        }
    }
    return $url;
}

=head2 _weighted_clusters

Get the weighted clusters for a config.

=cut

sub _weighted_clusters {
    my $self = shift;
    return $self->{'weighted_clusters'} if $self->{'weighted_clusters'};
    $self->{'weighted_clusters'} = [];
    for my $cluster (0..$#{$self->{'clusters'}}) {
        my $weighted_cluster = [];
        for my $bucket (0..$#{$self->{'clusters'}->[$cluster]}) {
            my $bucket_info = $self->{'clusters'}->[$cluster]->[$bucket];
            push(@$weighted_cluster,
                ($bucket) x $bucket_info->{'write_weight'});
        }
        push(@{$self->{'weighted_clusters'}}, $weighted_cluster);
    }
    return $self->{'weighted_clusters'};
}

=head2 _buckets

Get the buckets for an event.

=cut

sub _buckets {
    my $self = shift;
    my $event = shift;
    return $event->{'buckets'} if $event->{'buckets'};
    $event->{'buckets'} = {};
    if ((!exists($event->{'encoded'}) or $event->{'encoded'} == 1) and
        $self->{'encode_name'}) {
        $event->{'name'} =~ /0([^_]*)_.*/;
        my $encoded = $1;
        die "Invalid encoded name: $event->{name}" unless $encoded &&
            length($encoded) % 2 == 0;
        my $cluster = 0;
        while ($encoded =~ /(..)/g) {
            my $bucket = $BASE62->from_base($1);
            die "Invalid bucket in name: $bucket $event->{name}"
                unless $bucket < scalar(@{$self->{'clusters'}->[$cluster]});
            $event->{'buckets'}->{$cluster++} = $bucket;
        }
    }
    else {
        my $hash = hex(substr(Digest::MD5::md5_hex($event->{'name'}), 0, 8));
        my $weighted_clusters = $self->_weighted_clusters();
        for my $cluster (0..$#{$weighted_clusters}) {
            my $weighted_cluster = $weighted_clusters->[$cluster];
            $event->{'buckets'}->{$cluster} =
                $weighted_cluster->[$hash % scalar(@$weighted_cluster)];
        }
    }
    return $event->{'buckets'};
}

=head2 _replicas

Get the replicas for an event.

=cut

sub _replicas {
    my $self = shift;
    my $event = shift;
    return $event->{'replicas'} if $event->{'replicas'};
    $event->{'replicas'} = {};
    my $buckets = $self->_buckets($event);
    while ((my $cluster, my $bucket) = each(%$buckets)) {
        next if defined($self->{'cluster'}) && $self->{'cluster'} != $cluster;
        my $bucket_info = $self->{'clusters'}->[$cluster]->[$bucket];
        for (@{$bucket_info->{'replicas'}}) {
            $event->{'replicas'}->{$_} = 1;
        }
    }
    return $event->{'replicas'};
}

1;
