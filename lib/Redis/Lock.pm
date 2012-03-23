use strict;
use warnings;
use Carp;

package Redis::Lock;

sub new {
    my $class = shift;
    my %args  = @_;
    # Should we create the redis object internally and just detect Redis server like Redis.pm does?
    my $redis = delete $args{redis} || die;
    my $key   = delete $args{key} || die;
    my $value = delete $args{value} || '';

    if (%args) {
        Carp::croak('unrecognized arguments: ' . join(', ', keys %args));
    }

    unless ($redis) { Carp::croak("required argument 'redis' is missing or is undefined") }
    unless ($key)   { Carp::croak("required argument 'key' is missing or is undefined") }

    unless ($redis->isa('Redis')) { Carp::croak("supplied argument for 'redis' is not a Redis object") }

    return bless {
        redis => $redis,
        key   => $key,
        value => $value,
    }, $class;
}

sub redis { return shift->{redis} }
sub key   { return shift->{key}   }
sub value { return shift->{value} }

sub lock {
    my $self             = shift;
    my %args             = @_;
    my $refresh_interval = delete $args{refresh_interval};
    my $timeout          = delete $args{timeout};

    if (%args) {
        Carp::croak('unrecognized arguments: ' . join(', ', keys %args));
    }

    if ($refresh_interval) {
        if (!Scalar::Util::looks_like_number($refresh_interval)) {
            Carp::croak("supplied argument for 'refresh_interval' is not a number");
        }
        if ($refresh_interval < 0) {
            Carp::croak("supplied argument for 'refresh_interval' is less than zero");
        }
    }

    if ($timeout) {
        if (!Scalar::Util::looks_like_number($timeout)) {
            Carp::croak("supplied argument for 'timeout' is not a number");
        }
        if ($timeout < 0) {
            Carp::croak("supplied argument for 'timeout' is less than zero");
        }
    }

    # TODO Do we need to check connection to Redis?

    $self->redis->multi;
    $self->redis->setnx($self->key => $self->value);
    $self->redis->expire($self->key => 10);
    unless ($self->redis->exec) {
        return;
    }

    if ($timeout) {
        require heartbeat;
        $refresh_interval  ||= 1;
        my $key            = $self->key;
        # TODO Looked like a user could also create a Redis object with a socket handle so should validate this too.
        my ($ip, $port)    = split(':', $self->redis->{server});
        unless ($ip && $port) {
            die("failed to determine IP and port from Redis object");
        }
        $self->{heartbeat} = heartbeat::start_pacer($ip, $port, $key, $refresh_interval, $timeout);
    }

    return 1;
}

sub release {
    my $self = shift;

    unless ($self->redis->del($self->key)) {
        Carp::croak('failed to remove lock');
        return;
    }
    if ($self->{heartbeat}) {
        heartbeat::stop_pacer($self->{heartbeat});
    }

    return 1;
}

sub DESTROY { shift->release }

1;
