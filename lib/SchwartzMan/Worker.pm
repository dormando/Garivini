package SchwartzMan::Worker;

use strict;
use warnings;
use fields ('dbd',
           );

use SchwartzMan::DB;

# Extremely simple shim worker.
# Registers functions directly to gearman, and accepts jobs.
# TODO: Feels like a waste wrapping all of Gearman::Worker just for this
# thing, but given the way it is it feels like it's misnamed. Any ideas?
sub new {
    my SchwartzMan::Worker $self = shift;
    $self = fields::new($self) unless ref $self;
    my %args = @_;

    $self->{dbd} = SchwartzMan::DB->new(%args);
}

# On job complete or failure, calls one of these:

# Takes a job handle, issues a DELETE against the database directly.
# This avoids requiring the manager worker to jump through hoops and helps
# speed things up overall.
sub complete_job {
    my $self = shift;
    my $job  = shift;

    # Job should have the dbid buried in the reference.
    my $dbid = $job->{dbid}
        or die "Malformed job missing dbid argument";
    my $jobid = $job->{id}
        or die "Malformed job missing id argument";
    $self->{dbd}->do($dbid, "DELETE FROM job WHERE jobid=?", undef, $jobid);
}

# Bump the run_after in some specific way (relative, absolute, etc)
sub reschedule_job {

}

# Reschedule for ENDOFTIME
sub fail_job_permanently {

}
