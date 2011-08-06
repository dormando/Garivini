package SchwartzMan::Injector;

use strict;
use warnings;

use SchwartzMan::DB;
use SchwartzMan::Client;
use Gearman::Worker;
use Gearman::Client;
use JSON;

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
        job_servers => $job_servers);
    $self->{last_queue_check} = 0;
    $self->{queues} = {};
    $self->{queue_watermark_depth} = 4000; # TODO: Make configurable
}

# TODO: Think I'm still missing a part where the original job function is
# cuddled into the gearman job arguments somehow. Figure this out?
sub work {
    my %args = @_;

    my $worker = Gearman::Worker->new(job_servers => $self->{job_servers});
    $worker->register_function('SchwartzMan::Injector' => sub {
        $self->queue_jobs
    });
    $worker->work while 1; # redundant.
}

# For bonus points, submit jobs in this method with a run_after noted as
# "locked", so the client code can pre-adjust it and not be forced to
# re-encode.
sub queue_jobs {
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
    $args->{run_after} = $run_job && exists $args->{run_after} ? $args->{run_after}
        : 'UNIX_TIMESTAMP() + 1000'; # Sick, don't do this directly, here.

    $self->{sm_client}->insert_job($args);

    if ($run_job) {
        # TODO: Submit uniq properly.
        $self->{gm_client}->dispatch_background($args->{funcname}, \encode_json($args), {});
    }

    if ($last_queue_check < time() - 60) {
        $self->{queues} = $self->check_gearman_queues;
    }
}

# TODO: Copy/paste code from SchwartzMan.pm until Gearman::Client has a thing
# for this.
sub check_gearman_queues {

}

1;
