package SchwartzMan::Injector;

use strict;
use warnings;

use SchwartzMan::DB;
use SchwartzMan::Client;
use Gearman::Worker;
use Gearman::Client;
use JSON;
use Data::Dumper qw/Dumper/;

use fields (
            'job_servers',
            'sm_client',
            'gm_client',
            'last_queue_check',
            'queues', 
            'queue_watermark_depth',
           );

sub new {
    my SchwartzMan::Injector $self = shift;
    $self = fields::new($self) unless ref $self;
    my %args = @_;

    $self->{job_servers} = delete $args{job_servers};

    $self->{sm_client} = SchwartzMan::Client->new(%args);
    $self->{gm_client} = Gearman::Client->new(
        job_servers => $self->{job_servers});
    $self->{last_queue_check} = 0;
    $self->{queues} = {};
    $self->{queue_watermark_depth} = 4000; # TODO: Make configurable

    return $self;
}

sub work {
    my $self = shift;
    my $worker = Gearman::Worker->new(job_servers => $self->{job_servers});
    $worker->register_function(inject_jobs => sub {
        $self->inject_jobs(@_);
    });
    $worker->work while 1; # redundant.
}

# For bonus points, submit jobs in this method with a run_after noted as
# "locked", so the client code can pre-adjust it and not be forced to
# re-encode.
# TODO: If $job is an arrayref of other jobs, do a multi insert, but don't
# inject directly into run_queued_job. QueueRunner will pick them back out
# with the jobids intact.
sub inject_jobs {
    my $self = shift;
    my $job  = shift;

    my $queues = $self->{queues};
    my $args = decode_json(${$job->argref});
    my $run_job = 1;
    if ($queues->{$args->{funcname}} && $queues->{$args->{funcname}} >
        $self->{queue_watermark_depth}) {
        $run_job = 0;
    }

    # TODO: Uhh if $job->{arg} was a cuddled JSON blob, did this just decode
    # that? Easy enough to test and fix, but I'm tired :P
    $args->{run_after} = ($run_job && exists $args->{run_after}) ? $args->{run_after}
        : 'UNIX_TIMESTAMP() + 1000'; # Sick, don't do this directly, here.
    $args->{flag} = 'controller';
    my ($jobid, $dbid) = $self->{sm_client}->insert_job(%$args);

    $args->{jobid} = $jobid;
    $args->{dbid}  = $dbid;
    if ($run_job) {
        # TODO: Submit uniq properly.
        $self->{gm_client}->dispatch_background('run_queued_job',
            \encode_json($args), {});
    }

    if ($self->{last_queue_check} < time() - 60) {
        $self->{last_queue_check} = time();
        $self->{queues} = $self->check_gearman_queues;
    }

    return;
}

# TODO: Copy/paste code from SchwartzMan.pm until Gearman::Client has a thing
# for this.
sub check_gearman_queues {
    return {};
}

1;
