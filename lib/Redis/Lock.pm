use strict;
use warnings;

package Redis::Lock;

sub new {
	my $class = shift;
	my %args  = @_;
	my $redis = delete $args{redis} || die;
	my $key   = delete $args{key} || die;
	my $value = delete $args{value} || die;
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
	my $self  = shift;
	my %options          = @_;
	my $refresh_interval = delete $options{refresh_interval};
	my $timeout          = delete $options{timeout};

	unless ($self->redis->setnx($self->key => $self->value)) {
		return;
	}
	if ($timeout) {
		require heartbeat;
		my ($ip, $port)    = split(':', $self->redis->{server});
		my $key = $self->key;
		$self->{heartbeat} = heartbeat::start_pacer($ip, $port, $key, $refresh_interval, $timeout);
	}

	return 1;
}

sub release {
	my $self = shift;

	unless ($self->redis->del($self->key)) {
		return;
	}
	if ($self->{heartbeat}) {
		heartbeat::stop_pacer($self->{heartbeat});
	}

	return 1;
}

sub DESTROY { shift->release }

1;
