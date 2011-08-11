package SchwartzMan::QueueRunner;
use strict;
use warnings;

use fields (
            'job_servers',
            'gearman_client',
            'dbd',
            'gearman_sockets',
            'batch_fetch_limit',
            'batch_run_sleep',
            'queue_watermark_depth',
            );

use Carp qw/croak/;
use SchwartzMan::DB;
use Gearman::Client;
use IO::Socket;
use JSON;
use Data::Dumper qw/Dumper/;

# TODO: Make the "job" table name configurable:
# one dbid creates one connection, which is load balanced against N job tables
# need to test this, because in theory can get more throughput out of a single
# database by spreading the transaction locking anguish.

# Queue loading algorithm; pull minimum batch size, then grow up to the max
# size if queue isn't empty.
use constant MIN_BATCH_SIZE => 50;
use constant MAX_BATCH_SIZE => 2000;
use constant DEBUG => 0;

sub new {
    my SchwartzMan::QueueRunner $self = shift;
    $self = fields::new($self) unless ref $self;
    my %args = @_;

    $self->{job_servers}     = {};
    # Gross direct socket connections until Gearman::Client gives us a way to
    # get queue status information more efficiently.
    $self->{gearman_sockets} = {};

    # TODO: Configuration verification!
    $self->{dbd}            = SchwartzMan::DB->new(%args);
    $self->{job_servers}    = $args{job_servers};
    $self->{gearman_client} = Gearman::Client->new(
        job_servers => $args{job_servers});

    $self->{batch_fetch_limit} = MIN_BATCH_SIZE;
    $self->{batch_run_sleep} = $args{batch_run_sleep} || 1;
    $self->{queue_watermark_depth} = $args{queue_watermark_depth} ||
        4000;

    return $self;
}

# Blindly pull jobs that are due up out of the database, to inject into
# gearmand.
sub _find_jobs_for_gearman {
    my $self   = shift;
    my $limit  = shift;
    my $queues = shift;

    # Fetch this round from a random queue.
    my ($dbh, $dbid) = $self->{dbd}->get_dbh();
    my @joblist = ();
    my $pulled  = 0;

    eval {
        $dbh->begin_work;
        my $query = qq{
            SELECT jobid, funcname, uniqkey, coalesce, run_after, arg,
            $dbid AS dbid, flag, failcount
            FROM job
            WHERE run_after <= UNIX_TIMESTAMP()
            ORDER BY run_after
            LIMIT $limit
            FOR UPDATE
        };
        my $sth = $dbh->prepare($query);
        $sth->execute;
        my $work = $sth->fetchall_hashref('jobid');
        # Claim the jobids for a while
        @joblist     = ();
        my @skiplist = ();
        # If queue is full, don't skip run_after as far
        for my $work (values %$work) {
            $pulled++;
            if (exists $queues->{$work->{funcname}}
                && $queues->{$work->{funcname}} >
                $self->{queue_watermark_depth}) {
                push(@skiplist, $work->{jobid});
            } else {
                push(@joblist, $work);
            }
        }
        my $idlist   = join(',', map { $_->{jobid} }  @joblist);
        my $skiplist = join(',', @skiplist);

        # TODO: time adjustment should be configurable
        $dbh->do("UPDATE job SET run_after = UNIX_TIMESTAMP() + 1000 "
            . "WHERE jobid IN ($idlist)") if $idlist;
        $dbh->do("UPDATE job SET run_after = UNIX_TIMESTAMP() + 60 "
            . "WHERE jobid IN ($skiplist)") if $skiplist;
        $dbh->commit;
    };
    # Doesn't really matter what the failure is. Deadlock? We'll try again
    # later. Dead connection? get_dbh() should be validating.
    # TODO: Make sure get_dbh is validating, or set a flag here on error which
    # triggers get_dbh to run a $dbh->ping next time.
    if ($@) {
        DEBUG && print STDERR "DB Error pulling from queue: $@";
        eval { $dbh->rollback };
        return ();
    }

    return (\@joblist, $pulled);
}

# Fetch gearmand status (or is there a better command?)
# Look for any functions which are low enough in queue to get more injections.
sub _check_gearman_queues {
    my $self    = shift;
    my $servers = $self->{job_servers};
    my $socks   = $self->{gearman_sockets};
    my %queues  = ();

    # Bleh. nasty IO::Socket code to connect to the gearmand's and run "status".
    for my $server (@$servers) {
        my $sock = $socks->{$server} || undef;
        unless ($sock) {
            $sock = IO::Socket::INET->new(PeerAddr => $server,
                Timeout => 3);
            next unless $sock;
            $socks->{$server} = $sock;
        }
        eval {
            print $sock "status\r\n";

            while (my $line = <$sock>) {
                chomp $line;
                last if $line =~ m/^\./;
                my @fields = split(/\s+/, $line);
                $queues{$fields[0]} += $fields[1];
            }
        };
        if ($@) {
            # As usual, needs more complete error handling.
            $socks->{$server} = undef;
        }
    }

    return \%queues;
}

# Send all jobs via background tasks.
# We don't care about the response codes, as we rely on the gearman workers to
# issue the database delete.
sub _send_jobs_to_gearman {
    my $self = shift;
    my $jobs = shift;

    my $client = $self->{gearman_client};

    for my $job (@$jobs) {
        my $funcname;
        $job->{flag} ||= 'shim';
        if ($job->{flag} eq 'shim') {
            $funcname = $job->{funcname};
        } elsif ($job->{flag} eq 'controller') {
            $funcname = 'run_queued_job';
        } else {
            die "Unknown flag state $job->{flag}";
        }
        my %opts = ();
        $opts{uniq} = $job->{uniqkey} if $job->{uniqkey};
        $client->dispatch_background($funcname, \encode_json($job), \%opts);
    }
}

# This isn't your typical worker, as in it doesn't register as a gearman
# worker at all. It's a sideways client.
# TODO; split into work/work_once ?
sub work {
    my $self = shift;

    while (1) {
        my $queues = $self->_check_gearman_queues;
        my ($jobs, $pulled) = $self->_find_jobs_for_gearman($self->{batch_fetch_limit},
            $queues);
        if (!defined $jobs) {
            sleep $self->{batch_run_sleep}; next;
        }
        my $job_count = scalar @$jobs;
        DEBUG && print STDERR "Pulled $pulled new jobs from DB\n";
        DEBUG && print STDERR "Sending $job_count jobs to gearmand\n";
        $self->_send_jobs_to_gearman($jobs);

        if ($job_count >= $self->{batch_fetch_limit}) {
            $self->{batch_fetch_limit} += (MAX_BATCH_SIZE * 0.1);
            $self->{batch_fetch_limit} = MAX_BATCH_SIZE
                if $self->{batch_fetch_limit} >= MAX_BATCH_SIZE;
        } else {
            $self->{batch_fetch_limit} -= (MAX_BATCH_SIZE * 0.05);
            $self->{batch_fetch_limit} = MIN_BATCH_SIZE
                if $self->{batch_fetch_limit} <= MIN_BATCH_SIZE;
        }

        DEBUG && print STDERR "Sent, sleeping\n";
        # Sleep for configured amount of time.
        # TODO: Use the select microsleep hack?
        sleep $self->{batch_run_sleep};
    }
}

1;
