#!/usr/bin/perl

use strict;
use warnings;

use DateTime;
use Search::Elasticsearch;
use GRNOC::WebService::Client;
use GRNOC::Config;
use Data::Dumper;
use List::MoreUtils;
use Getopt::Long;
use JSON;

my $start;
my $config_file;

my $USAGE = "$0 --config <config file> [--start <start epoch, will import from start to now, default is yesterday>]";

GetOptions("start=i" => \$start,
           "config=s" => \$config_file ) or die "$!\n$USAGE";

die $USAGE unless ($config_file);

my $config = GRNOC::Config->new(config_file => $config_file);

my $user     = $config->get('/config/tsds/@user')->[0];
my $pass     = $config->get('/config/tsds/@password')->[0];
my $tsds_url = $config->get('/config/tsds/@url')->[0];

my $elastic_host = $config->get('/config/elasticsearch/@host')->[0];
my $elastic_port = $config->get('/config/elasticsearch/@port')->[0];

my $keys = $config->get('/config/aggregate/key');

# Align to 1 day boundaries
$start = time() - 86400 if (! defined $start);

$start = int($start / 86400) * 86400;
my $now = time();

my $wsc = GRNOC::WebService::Client->new(usePost => 1,
                                         uid     => $user,
                                         passwd  => $pass);
                                        
my $es = Search::Elasticsearch->new(nodes => "$elastic_host:$elastic_port");

while ($start < $now){
    $start +=  86400; 

    my $datetime = DateTime->from_epoch(epoch => $start);
    my $year     = $datetime->year();
    my $month    = $datetime->month();
    my $day      = $datetime->day();
    
    $datetime->truncate(to => 'day');
    my $submit_time = $datetime->epoch();
    
    
    if ($day < 10){
        $day = "0" . $day;
    }
    if ($month < 10){
        $month = "0" . $month;
    }
        
    my $results;
    eval {
        $results = $es->search(
            index => "netsage-" . $year . "." . $month . "." . $day,
            type  => 'flow'
            );
    };
    if ($@){
        print "Unable to fetch data from elasticsearch: $@\n";
    }
    next if (! $results);

    # do the work for each of the aggregation keys asked for
    foreach my $key (@$keys){

        # if the key looks like src_organization,dst_organization we're going to make a pair
        my @pairs = split(/,/, $key);

        print "Making aggregates for key = $key\n";

        my %crunched_singles;
        my %crunched_pairs;

        # Run through every elasticsearch result and crunch those numbers
        foreach my $result (@{$results->{'hits'}{'hits'}}){
            
            my $metadata = $result->{'_source'}{'meta'};
            my $data     = $result->{'_source'}{'values'};
            
            my $num_packets = $data->{'num_packets'};
            my $num_bits    = $data->{'num_bits'};
            
            my $point_of_obs = $metadata->{'sensor_id'};            

            # If this key was NOT a paired key, just make the single result
            if (@pairs == 1){
                my $key_val = $metadata->{$key};
                
                if (! $key_val){
                    print "Unable to find value for $key in record, skipping\n";
                    next;
                }
                
                $crunched_singles{$point_of_obs}{$key_val}{'bits'} += $num_bits;
                
                # We also want to include a "*" point of observation that represents the totality
                # of each flow on every sensor. There may be duplication of traffic here.
                $crunched_singles{'*'}{$key_val}{'bits'} += $num_bits;
            }
            # If this key WAS a paired key, make the paired result
            else {
                my $key_val_a = $metadata->{$pairs[0]};
                my $key_val_b = $metadata->{$pairs[1]};

                if (! $key_val_a || ! $key_val_b){
                    print "Unable to find values for either $pairs[0] or $pairs[1] in record, skipping\n";
                    next;
                }

                $crunched_pairs{$point_of_obs}{$key_val_a}{$key_val_b}{'output'} += $num_bits;
                $crunched_pairs{$point_of_obs}{$key_val_b}{$key_val_a}{'input'} += $num_bits;

                $crunched_pairs{'*'}{$key_val_a}{$key_val_b}{'output'} += $num_bits;
                $crunched_pairs{'*'}{$key_val_b}{$key_val_a}{'input'} += $num_bits;
            }
        }    
    
        my @to_send;
        my @meta_send;        

        # Now that we have collapsed all the records down, go through
        # and formulate all the necessary TSDS messages.
        # This DOES assume that the measurement types have already been made in
        # TSDS, but is harmless if they haven't been, just won't add any data
        foreach my $point_of_obs (keys %crunched_singles){
            
            my $tsds_type = "netflow_" . $key;
            
            my $data = $crunched_singles{$point_of_obs};
            
            foreach my $meta_val (keys %$data){
                my $bits = 0 + ($data->{$meta_val}{'bits'} || 0);
                
                push(@to_send, {
                    interval => 86400,
                    type => $tsds_type,
                    time => $submit_time,
                    meta => {
                        $key => $meta_val,                    
                        "point_of_observation" => $point_of_obs
                    },
                    values => {
                        bits => $bits
                    }            
                     });
                
                push(@meta_send, {
                    type => $tsds_type,
                    start => $submit_time,
                    end => $submit_time + 86400,
                    $key => $meta_val,
                    "point_of_observation" => $point_of_obs
                     });            
            }        
        }

        # Same thing but for any paired aggregates
        foreach my $point_of_obs (keys %crunched_pairs){
            
            my $tsds_type = "netflow_" . $pairs[0] . "_" . $pairs[1];
            
            my $data = $crunched_pairs{$point_of_obs};          
            
            foreach my $meta_val_a (keys %$data){
                foreach my $meta_val_b (keys %{$data->{$meta_val_a}}){
                    my $input = 0 + ($data->{$meta_val_a}->{$meta_val_b}->{'input'} || 0);
                    my $output = 0 + ($data->{$meta_val_a}->{$meta_val_b}->{'output'} || 0);
                
                    push(@to_send, {
                        interval => 86400,
                        type => $tsds_type,
                        time => $submit_time,
                        meta => {
                            $pairs[0] => $meta_val_a,                    
                            $pairs[1] => $meta_val_b,                    
                            "point_of_observation" => $point_of_obs
                        },
                        values => {
                            input => $input,
                            output => $output
                        }            
                     });
                
                    push(@meta_send, {
                        type => $tsds_type,
                        start => $submit_time,
                        end => $submit_time + 86400,
                        $pairs[0] => $meta_val_a,
                        $pairs[1] => $meta_val_b,
                        "point_of_observation" => $point_of_obs
                         });            
                }        
            }
        }

        # Go through and send our data over to TSDS
        $wsc->set_url($tsds_url . "/services/push.cgi");
        my $it = List::MoreUtils::natatime(10, @to_send);
        while (my @block = $it->()){

            print "Sending " . scalar(@block) . " data messages...\n";

            my $res = $wsc->add_data(data => JSON::encode_json(\@block));
            
            if (! $res){
                die Dumper($wsc->get_error());
            }
        }
    
     
        # This is so hacky since add_data is asynch, needs to be enhanced.
        # Gives add_data time to finish on the backend
        sleep(10);
        

        # Now that data has been added, signal end of the metadata record
        # so that we keep nice day boundaries
        $wsc->set_url($tsds_url . "/services/admin.cgi");                
        $it = List::MoreUtils::natatime(10, @meta_send);
        while (my @block = $it->()){

            print "Sending " . scalar(@block) . " metadata messages...\n";

            my $res = $wsc->update_measurement_metadata(values => JSON::encode_json(\@block));
            
            if (! $res){
                die Dumper($wsc->get_error());
            }
        }
    }
}
