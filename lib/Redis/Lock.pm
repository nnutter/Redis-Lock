use strict;
use warnings;

package Redis::Lock;

sub new {
    my $class = shift;
    my %args  = @_;

    my $redis = delete $args{redis};
    return bless {
        redis => $redis,
    }, $class;
}

sub lock {
    my $self  = shift;
    my $key   = shift;
    my $value = shift;

    my %options          = @_;
    my $keep_alive       = delete $options{keep_alive};
    my $refresh_interval = delete $options{refresh_interval};
    my $timeout          = delete $options{timeout};

    $self->redis->setnx($key => $value);
    if ($keep_alive) {
        require heartbeat;
        my ($ip, $port)    = split(':', $self->redis->{server});
        $self->{heartbeat} = heartbeat::start_pacer($ip, $port, $key, $refresh_interval, $timeout);
    }

    return;
}

sub release {
    my $self = shift;
    my $key  = shift;

    $self->redis->del($key);
    if ($self->{heartbeat}) {
        heartbeat::stop_pacer($self->{heartbeat});
    }

    return;
}

1;
