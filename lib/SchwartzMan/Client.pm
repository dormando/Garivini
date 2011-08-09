package SchwartzMan::Client;

use strict;
use warnings;
use fields ('dbd',
            );

use SchwartzMan::DB;

# Extremely simple shim client:
# Takes list of databases
# Takes serialized argument (or optional serialization command?)

sub new {
    my SchwartzMan::Client $self = shift;
    $self = fields::new($self) unless ref $self;
    my %args = @_;

    $self->{dbd} = SchwartzMan::DB->new(%args);

    return $self;
}

# Very bland DBI call.
# dbid => send to a specific DB (remove this? seems dangerous)
# job  => { funcname => $name,
#           run_after => $timestamp, (optional, defaults to UNIX_TIMESTAMP())
#           unique => $unique, (string)
#           coalesce => $key, (coalesce key, like the e-mail domain)
#           arg => $blob, (serialized blob)
#         };
# 'flag' can be 'shim' (default), or 'controller'
# 'shim' tells the QueueRunner that the worker is responsible for removing the
# job from the database upon completion.
# 'controller' tells it to inject the job through a controller worker.
sub insert_job {
    my $self = shift;
    my %args = @_;
    $args{unique}    = undef unless $args{unique};
    $args{coalesce}  = undef unless $args{coalesce};
    $args{flag}      = undef unless $args{flag};
    # FIXME: Verify $run_after as an argument, or else we have an injection
    # issue.
    my $run_after = $args{run_after} || 'UNIX_TIMESTAMP()';
    my ($ret, $dbh, $dbid) = $self->{dbd}->do(undef,
        "INSERT INTO job (funcname, run_after, uniqkey, coalesce, arg, flag) "
        . "VALUES (?, $run_after, ?, ?, ?, ?)", undef,
        @args{'funcname', 'unique', 'coalesce', 'arg', 'flag'});
    return ($dbh->last_insert_id(undef, undef, undef, undef), $dbid);
}

# Further potential admin commands:
sub list_jobs {

}

# Pull jobs scheduled for ENDOFTIME
sub failed_jobs {

}

# On job complete or failure, call one of these:

# Takes a job handle, issues a DELETE against the database directly.
sub complete_job {
    my $self = shift;
    my $job  = shift;

    # Job should have the dbid buried in the reference.
    my $dbid = $job->{dbid}
        or die "Malformed job missing dbid argument";
    my $jobid = $job->{jobid}
        or die "Malformed job missing id argument";
    $self->{dbd}->do($dbid, "DELETE FROM job WHERE jobid=?", undef, $jobid);
}

# Bump the run_after in some specific way (relative, absolute, etc)
sub reschedule_job {
    my ($self, $job, $when, $fail) = @_;

    my $dbid = $job->{dbid}
        or die "Malformed job missing dbid argument";
    my $jobid = $job->{jobid}
        or die "Malformed job missing id argument";

    if ($when eq 'never') {
        $when = 2147483647; # "max value"
    } elsif ($when =~ m/^\+(\d+)$/) {
        $when = 'UNIX_TIMESTAMP() + ' . $1;
    } elsif ($when =~ m/^(\d+)$/) {
        $when = $1;
    } else {
        die "Invalid timestamp: " . $when;
    }

    my $failcount = $job->{failcount} || 0;
    $self->{dbd}->do($dbid, "UPDATE job SET run_after = $when, failcount = ? WHERE jobid=?",
        undef, $failcount, $jobid);
}

# Reschedule with an arbitrary backoff.
sub failed_job {
    my ($self, $job) = @_;
    my $failcount = ($job->{failcount} || 0) + 1;
    my $delay = (2 ** $failcount) * 60;
    $delay = 86400 if $delay > 86400;

    $self->reschedule_job($job, "+$delay");
}

1;
