use strict;
use warnings;
use Carp;

package Redis::Lock;

sub new {
    my $class = shift;
    my %args  = @_;
    # TODO Validate redis object and key.
    # Should we create the redis object internally and just detect Redis server like Redis.pm does?
    my $redis = delete $args{redis} || die;
    my $key   = delete $args{key} || die;
    my $value = delete $args{value} || '';
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
    my %options          = @_;
    my $refresh_interval = delete $options{refresh_interval};
    my $timeout          = delete $options{timeout};

    # TODO Do we need to check connection to Redis?

    $self->redis->multi;
    $self->redis->setnx($self->key => $self->value);
    $self->redis->expire($self->key => 10);
    unless ($self->redis->exec) {
        return;
    }

    if ($timeout) {
        require heartbeat;
        # TODO Validate timeout.
        $refresh_interval  ||= 1;
        my $key            = $self->key;
        # TODO Looked like a user could also create a Redis object with a socket handle so should validate this too.
        my ($ip, $port)    = split(':', $self->redis->{server});
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
