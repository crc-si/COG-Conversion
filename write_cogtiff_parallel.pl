#!/usr/bin/env perl
use Parallel::ForkManager;
use feature 'say';

$out_f_name = $ARGV[0];
$outdir = $ARGV[1];
$netcdf = $ARGV[2];
$rastercount = $ARGV[3];

my $MAX_PROCESSES = $rastercount;
my $pm = Parallel::ForkManager->new($MAX_PROCESSES);
for($count=1; $count <= $rastercount; $count++)
{
    push(@runArray, "./write_cogtiff.py $out_f_name $outdir $netcdf $count $rastercount");
}
$len = $#runArray;
print "Len = $len\n";
for $line (@runArray)
{
    print "$line\n";
}
print "Len = $len\n";
    $pm->run_on_start( sub {
        ++$number_running;
        say "Started $_[0], total: $number_running";
    });
    $pm->run_on_finish( sub {
        --$number_running;
        my ($pid, $code, $iden, $sig, $dump, $rdata) = @_;
        push @ds, "gone-$pid";
        say "Cleared $pid, total: $number_running ", ($rdata->[0] // ''), ($code ? " exit $code" : '');
    });
    my $j =0;
    for my $runCommand (@runArray) {
        $j++;
        print "Run: $j. $runCommand\n";
        $pm->start($runCommand) and next;

        exec("$runCommand") or die("exec: $!");
    }

    $pm->wait_all_children;

    sleep 10;

