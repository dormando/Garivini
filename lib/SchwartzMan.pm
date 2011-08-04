package SchwartzMan;
use strict;
use warnings;

use fields (
            'job_servers',
            'gearman_client',
            'dbd',
            'gearman_sockets',
            'batch_fetch_limit',
            'batch_run_sleep',
            );

use Carp qw/croak/;
use SchwartzMan::DB;
use Gearman::Client;
use IO::Socket;

# TODO: Make the "job" table name configurable:
# one dbid creates one connection, which is load balanced against N job tables
# need to test this, because in theory can get more throughput out of a single
# database by spreading the transaction locking anguish.

# Queue loading algorithm; pull minimum batch size, then grow up to the max
# size if queue isn't empty.
use constant MIN_BATCH_SIZE => 50;
use constant MAX_BATCH_SIZE => 2000;

sub new {
    my SchwartzMan $self = shift;
    $self = fields::new($self) unless ref $self;
    my %args = @_;

    # pull in options:
    # list of databases (dbid/dsn/user/pass)

    $self->{job_servers}     = {};
    $self->{funcmap_cache}   = {};
    # Gross direct socket connections until Gearman::Client gives us a way to
    # get queue status information more efficiently.
    $self->{gearman_sockets} = {};

    # TODO: Configuration verification!
    $self->{dbd}            = SchwartzMan::DB->new(%args);
    $self->{job_servers}    = $args{job_servers};
    $self->{gearman_client} = Gearman::Client->new(
        job_servers => $args{job_servers});

    $self->{batch_fetch_limit} = MIN_BATCH_SIZE;
    $self->{batch_run_sleep} = 1;

    return $self;
}

# Blindly pull jobs that are due up out of the database, to inject into
# gearmand.
# Use the MogileFS model: SELECT ... FOR UPDATE; UPDATE (etc); COMMIT;
# NOTE: May have to cycle/alternate by funcid?
# *should* actually be fine if we strictly order the SELECT by run_after.
sub _find_jobs_for_gearman {
    my $self  = shift;
    my $limit = shift;

    # Fetch this round from a random queue.
    my ($dbh, $dbid) = $self->{dbd}->get_dbh();
    my $work;

    # TODO: Split into two UPDATE's, filtered by which queues are too full for
    # more jobs.
    eval {
        $dbh->begin_work;
        my $query = qq{
            SELECT jobid, funcname, uniqkey, coalesce, run_after, arg,
            $dbid AS dbid
            FROM job
            WHERE run_after <= UNIX_TIMESTAMP()
            ORDER BY nexttry
            LIMIT $limit
            FOR UPDATE
        };
        my $sth = $dbh->prepare($query);
        $sth->execute;
        $work = $sth->fetchall_hashref('jobid');

        # Claim the jobids for a while
        # TODO: time adjustment should be configurable
        my $idlist = join(',', keys %$work);
        # Nothing to do
        unless ($idlist) { $dbh->commit; return; }
        $dbh->do("UPDATE job SET nexttry = UNIX_TIMESTAMP() + 1000 "
            . "WHERE jobid IN ($idlist)");
        $dbh->commit;
    };
    # Doesn't really matter what the failure is. Deadlock? We'll try again
    # later. Dead connection? get_dbh() should be validating.
    # TODO: Make sure get_dbh is validating, or set a flag here on error which
    # triggers get_dbh to run a $dbh->ping next time.
    if ($@) {
        eval { $dbh->rollback };
        return ();
    }

    return $work;
}

# Fetch gearmand status (or is there a better command?)
# Look for any functions which are low enough in queue to get more injections.
sub _check_gearman_queues {
    my $self    = shift;
    my @servers = $self->{job_servers};
    my $socks   = $self->{gearman_sockets};
    my %queues  = ();

    # Bleh. nasty IO::Socket code to connect to the gearmand's and run "status".
    for my $server (@servers) {
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
                # TODO: I don't know what the status line looks like offhand
                # :P
                $queues{'func'} += 0;   
                last if $line =~ m/^\./;
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
# Alternate design could have this worker create a taskset and wait on the
# taskset; tradeoff would be pretty painful though, as we can't necessarily
# inject more jobs into gearmand from this worker until all the previous jobs
# are done processing.
sub _send_jobs_to_gearman {
    my $self = shift;
    my $jobs = shift;

    my $client = $self->{gearman_client};

    # TODO: Need to pass uniq in properly?
    for my $job (@$jobs) {
        # TODO: Must serialize the job first; JSON?
        $client->dispatch_background($job->{funcname}, $job->{arg}, {});
    }
}

# The above should be most of what's required.
# Jobs should be extremely simple; allow clients the option to log errors
# somewhere, but they really should be using other logging facilities.
# Just drop all of the schwartz's job complexity on the floor since I'm sure
# most of it didn't work very well anyway.

# This isn't your typical worker, as in it doesn't register as a gearman
# worker at all. It's a sideways client.
sub work {
    my $self = shift;

    while (1) {
        my $queues = $self->_check_gearman_queues;
        my $work = $self->_find_jobs_for_gearman($self->{batch_fetch_limit});
        my $work_count = scalar keys %$work;

        # Filter jobs that should not be run
        my @jobs_tosend = ();
        for my $job (values %$work) {
            my $queue = $queues->{$job->{funcname}};
            next if ($queue > $self->{queue_watermark_depth})
            push(@jobs_tosend, $job);
        }

        # Send jobs to gearman
        $self->_send_jobs_to_gearman(\@jobs_tosend);

        # TODO:
        # Apply scaling algorithm: If gearman queues could use more work, and
        # $limit objects are found during fetch, and any gearmand queues can allow
        # more work, increase limit toward max.
        # if $limit objects are not found during fetch, slowly decrease limit
        # toward minimum
        if ($work_count >= $self->{batch_fetch_limit}) {
            $self->{batch_fetch_limit} += 10
                unless $self->{batch_fetch_limit} >= MAX_FETCH_SIZE;
        }

        # Sleep for configured amount of time.
        # TODO: Use the select microsleep hack?
        sleep $self->{batch_run_sleep};
    }
}
