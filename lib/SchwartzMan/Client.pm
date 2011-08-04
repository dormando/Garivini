package SchwartzMan::Client

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
}

# Very bland DBI call.
# dbid => send to a specific DB (remove this? seems dangerous)
# job  => { function => $name,
#           run_after => $timestamp, (optional, defaults to UNIX_TIMESTAMP())
#           unique => $unique, (string)
#           coalesce => $key, (coalesce key, like the e-mail domain)
#           arg => $blob, (serialized blob)
#         };
# TODO: Is there value in returning the jobid? If so, adding it won't be hard.
sub insert_job {
    my $self = shift;
    my %args = @_;
    $args{unique}    = 'NULL' unless $args{unique};
    $args{coalesce}  = 'NULL' unless $args{coalesce};
    $args{run_after} = 'UNIX_TIMESTAMP()' unless $args{run_after};
    $self->{dbd}->do(undef,
        "INSERT INTO job (funcname, run_after, uniqkey, coalesce, arg) "
        . "VALUES (?, ?, ?, ?, ?)", undef,
        @arg{'funcname', 'run_after', 'unique', 'coalesce', 'arg'});
}

# Just in case?
sub cancel_job {
    my $self  = shift;
}

# Further potential admin commands:
sub list_jobs {

}

# Pull jobs scheduled for ENDOFTIME
sub failed_jobs {

}
