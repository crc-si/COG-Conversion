#!/usr/bin/env perl
use Parallel::ForkManager;
use feature 'say';

#$netcdf_path = $ARGV[0];
#$output_dir = $ARGV[1];
my $netcdf_path = "/g/data/u46/users/sa9525/avs/STAC/FC/Parallel/Input/";
my $output_dir = "/g/data/u46/users/sa9525/avs/STAC/FC/Parallel/Output/";

opendir my $dir, $netcdf_path or die "Cannot open directory: $!";
my @files = readdir $dir;
closedir $dir;

my $MAX_PROCESSES = $#files + 1;
#my $MAX_PROCESSES = 10;
my $pm = Parallel::ForkManager->new($MAX_PROCESSES);
my @runArray = ();
    foreach my $fname (@files) 
    {
        if ($fname !~ /\.nc/i) { next; }
        $fname = $netcdf_path . $fname;
        push(@runArray, "./create_cog.py $fname $output_dir");
    }
    $len = $#runArray;
    print "Len = $len\n";
    for $line (@runArray)
    {
        print "$line\n";
    }
    print "Len = $len\n";
   
#while (1) {
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
#}
=pod
DATA_LOOP:
    foreach my $fname (@files) 
    {
        if ($fname !~ /\.nc/i) { next; }
        $fname = $netcdf_path . $fname;
        # Forks and returns the pid for the child:
        my $pid = $pm->start and next DATA_LOOP;
        $res = `./create_cog.py $fname $output_dir`;
        sleep(30);
#        print "$res | ";
        $pm->finish; # Terminates the child process
    }
=cut
