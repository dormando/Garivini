package SchwartzMan::Injector;

use strict;
use warnings;

use SchwartzMan::DB;
use SchwartzMan::Client;
use Gearman::Worker;
use Gearman::Client;
use JSON;

use constant GEARMAN_QUEUE_CHECK => 5;
use constant GEARMAN_QUEUE_SIZE_MAX => 2000; # shouldn't be a constant.
# TODO: Load config from somewhere.
# Or turn into a full object so somethign else can fire it off, but that's a
# little self defeating?

my $job_servers = [];
my $sm_client;
my $gm_client;
my $json;
my $last_queue_check = 0;
my $queues = {};

sub init {
    my %args = @_;
    $job_servers = delete $args{job_servers};

    $sm_client = SchwartzMan::Client->new(%args);
    $gm_client = Gearman::Client->new(
        job_servers => $job_servers);
    $json      = JSON->new->allow_nonref;

    my $worker = Gearman::Worker->new(job_servers => $job_servers);
    $worker->register_function('SchwartzMan::Injector' => \&queue_jobs);
    $worker->work while 1; # redundant.
}

# For bonus points, submit jobs in this method with a run_after noted as
# "locked", so the client code can pre-adjust it and not be forced to
# re-encode.
sub queue_jobs {
    my $job = shift;

    my $args = $json->decode(${$job->argref});
    my $run_job = 1;
    if ($queues->{$args->{funcname}} && $queues->{$args->{funcname}} >
        GEARMAN_QUEUE_SIZE_MAX) {
        $run_job = 0;
    }

    # TODO: Uhh if $job->{arg} was a cuddled JSON blob, did this just decode
    # that? Easy enough to test and fix, but I'm tired :P
    $args->{run_after} = $run_job && exists $args->{run_after} ? $args->{run_after}
        : 'UNIX_TIMESTAMP() + 1000'; # Sick, don't do this directly, here.

    $sm_client->insert_job($args);

    if ($run_job) {
        my $new_job = $json->encode($args);
        # TODO: Submit uniq properly.
        $gm_client->dispatch_background($args->{funcname}, $new_job, {});
    }

    if ($last_queue_check < time() - 60) {
        $queues = check_gearman_queues;
    }
}
