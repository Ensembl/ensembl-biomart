# Copyright [2009-2014] EMBL-European Bioinformatics Institute
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#      http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

use strict;
use warnings;

use Getopt::Std;

use Bio::EnsEMBL::Registry;

use EGVar::egv_utils;


my @opt_details =
    (['mysql_base_cmd', 'm', 's', 'r'],
    ['database', 'd', 's', 'r'],
    ['table', 't', 's', 'r'],
    ['dumps_file', 'u', 's', 'r'],
    ['bsub_queue_name', 'q', 's', 'o', 'production-rh6'],
    ['bsub_wait_status', 'w', 's', 'o', 'ended']);

my %prog_args;

EGVar::egv_utils::get_long_prog_args_from_opt(\@opt_details, \%prog_args);


print "\n" . EGVar::egv_utils::get_started_str() . "\n\n";


my $dump_file;
my $num_dumps;
my $base_load_cmd = "$prog_args{mysql_base_cmd} $prog_args{database} ";
my ($load_cmd, $bsub_cmd);
my $grep_cmd;
my $table_dump_file;
my $base_job_id = $prog_args{database} . '.' . $prog_args{table} . '_';
my ($job_id, $prev_job_id);

open DUMPS, $prog_args{dumps_file};

foreach $dump_file (<DUMPS>)
{
    chomp $dump_file;
    $num_dumps++;

    if ($dump_file !~ /^#/)
    {
        $table_dump_file = "$dump_file." . $prog_args{table};
        $grep_cmd = "grep '^INSERT.*$prog_args{table}' $dump_file > $table_dump_file";
        # print "$grep_cmd\n\n"; 
        system($grep_cmd);

        $load_cmd = "'$base_load_cmd < $table_dump_file'";
        $job_id = $base_job_id . $num_dumps;

        $bsub_cmd = "bsub -q$prog_args{bsub_queue_name} " .
            ($prev_job_id ? 
                "-w '$prog_args{bsub_wait_status}($prev_job_id)' " : '') .
            "-J$job_id -o$table_dump_file.out $load_cmd";
        # print "$bsub_cmd\n\n"; 
        system($bsub_cmd);

        $prev_job_id = $job_id;
    }
}

close DUMPS;


$job_id = $base_job_id . 'wait';

$bsub_cmd = 'bsub -K -w ' . "'$prog_args{bsub_wait_status}($base_job_id*)' " .
    "-q$prog_args{bsub_queue_name} -J$job_id -o$table_dump_file" . 
    '.wait.out sleep 1';
# print "$bsub_cmd\n"; 
system($bsub_cmd);


print "\n\n" . EGVar::egv_utils::get_ended_str() . "\n\n";


exit;

