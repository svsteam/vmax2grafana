#!/usr/bin/perl -w
use strict;
use IO::Socket::INET;
use LWP;
use HTTP::Request::Common;
use JSON;
use Data::Dumper;
# copyleft under GNU GPL 3.0 or later
# fs@mond.at
#


my $carbon_server = '127.1.2.3'; # ADD your carbon server here
my $carbon_port = 8086;
my $prefix='storage.vmax';
my $baseip="127.3.4.5:8443";
my $baseurl="https://$baseip/univmax/restapi/"; #ADD your URL here
my $baseuser="read-only-user-for-api-add-your-own";
my $basepw="super-secret-password";

my $mysym;

if (-e "v2g.lck" ) {
  my @lockstat=stat("v2g.lck");
  my $lage=time()-$lockstat[9];
  print "lock age is $lage at " . localtime() . "\n"; 
  if ($lage < 900 ) {
    exit(1);
  } else {
    print "breaking lock at " . localtime() . "\n";
    unlink("v2g.lck");
  }
}
open(L,">v2g.lck");
print L $$,"\n";
close(L);

my $sock = IO::Socket::INET->new(
        PeerAddr => $carbon_server,
        PeerPort => $carbon_port,
        Proto    => 'tcp'
);
die "Unable to connect: $!\n" unless ($sock->connected);

print "started at " . localtime() . "\n";                        
my $symids;

# you need a file with symid,boxname 
open(S,"<symtab.txt") or die "$!";
while(<S>) {
  chomp;
  my @f=split(',');
  $symids->{$f[0]}=$f[1];
}
close(S);

my $ua = LWP::UserAgent->new(keep_alive=>0);
$ua->proxy(['http','https'],'');
$ua->agent('Mozilla/5.0');
$ua->cookie_jar({});
$ua->credentials($baseip, 'EMCUNIVMAX-restapi', $baseuser, $basepw);
$ua->ssl_opts( 'verify_hostname' => 0  );
$ua->default_header('Content-Type' => "application/json");


my $response = $ua->get($baseurl . "performance/Array/keys");
die $response->status_line unless ($response->is_success);
# print  $response->content();
my $res=decode_json $response->content();

my $info=$res->{'arrayInfo'};
#print Dumper($info);
         #'firstAvailableDate' => '1465388100000',
         #          'symmetrixId' => '000296700588',
         #                    'lastAvailableDate' => '1466684700000'
                             
foreach my $k (@$info) {
  # print "-" x 40 , "\n",  Dumper($k); 
  my $symid=$k->{'symmetrixId'};
  my $first=$k->{'firstAvailableDate'} / 1000;
  my $last=$k->{'lastAvailableDate'} / 1000;
  my $uptime=$last-$first;
  my $age=time()-$last;
  # print "symid=$symid\n";
  my $sym=$symids->{$symid};
  if (defined $sym and $uptime > 1000 and $age < 1000 ) {
    $mysym->{$symid}->{'name'}=$sym;
    $mysym->{$symid}->{'last'}=$last;
    
    print "found sym: $sym, uptime: $uptime, age: $age , at " . localtime() . "\n";
  } else {
    print "undifined: symid=$symid uptime=$uptime age=$age, at " . localtime() . "\n";
    # print "undef $symid\n";
  }
  # print "key $k = val ",$info->{$k},"\n";
}

foreach my $symid (keys %$mysym) {
  my $sym=$mysym->{$symid}->{'name'};
  my $last=$mysym->{$symid}->{'last'};
  # {
  #  "startDate": "1466673000000",
  #   "endDate": "1466773000000",
  #    "symmetrixId": "000292602827"
  #    }
  #print "requesting sym $symid\n";
  my $perlreq;
  $perlreq->{'startDate'}=($last-300)*1000;
  $perlreq->{'endDate'}=$last*1000;
  $perlreq->{'symmetrixId'}=$symid;
  print "requesting array metrics for  $symid at " . localtime() . "\n";
  $perlreq->{'metrics'}=['HostIOs','HostReads','HostWrites','HostMBs','HostMBReads','HostMBWritten','ReadResponseTime','WriteResponseTime','CriticalAlertCount','WarningAlertCount','AllocatedCapacity'];
  my $req = encode_json  $perlreq;
  # print "\n\nreq=\'$req\'\n";

  # my $response = $ua->post($baseurl . "performance/Array/metrics", Contentx => $req  );
  my $response = $ua->request(POST $baseurl . "performance/Array/metrics", Content_Type => 'application/json',  Content => $req);
  # print $response->content();
  die $response->status_line unless ($response->is_success);
  my $res=decode_json $response->content();
  # print Dumper($res);
  my $resvalsarray=$res->{'resultList'}->{'result'};
  my $resvals=@$resvalsarray[0];
  #if (defined $resvals) {
  #  print Dumper($resvals),"-" x 40, "\n";
  #}  
  
  if (defined $resvals) {
    my $ts=int($resvals->{'timestamp'}/1000);
    foreach my $rk (keys %$resvals) {
      if ($rk ne 'timestamp') {
        my $val=$resvals->{$rk} + 0;
        print $sock "$prefix.$sym.array.$rk $val $ts\n";
      }  
    }
  }    
}


foreach my $symid (keys %$mysym) {
  my $sym=$mysym->{$symid}->{'name'};
  my $last=$mysym->{$symid}->{'last'};
  # {
  #  "startDate": "1466673000000",
  #   "endDate": "1466773000000",
  #    "symmetrixId": "000292602827"
  #    }
  
  my $perlreq;
  $perlreq->{'startDate'}=($last-600)*1000;
  $perlreq->{'endDate'}=$last*1000;
  $perlreq->{'symmetrixId'}=$symid;
  my $req = encode_json  $perlreq;
  # print "\n\nreq=\'$req\'\n";
  print "requesting host metrics for  $symid at " . localtime() . "\n";

  # my $response = $ua->post($baseurl . "performance/Array/metrics", Contentx => $req  );
  my $response = $ua->request(POST $baseurl . "81/performance/Host/keys", Content_Type => 'application/json',  Content => $req);
  # print $response->content();
  if ($response->is_success) {
     my $res=decode_json $response->content();
     my $ighosts=$res->{'hostInfo'};
     foreach my $k (@$ighosts) {
       my $hostid=$k->{'hostId'}; 
        my $hostreq;
        $hostreq->{'startDate'}=($last-300)*1000;
        $hostreq->{'endDate'}=$last*1000-1;
        $hostreq->{'symmetrixId'}=$symid;
        $hostreq->{'hostId'}=$hostid;
        $hostreq->{'metrics'}=['HostMBReads','HostMBWrites','Reads','Writes','ReadResponseTime','WriteResponseTime'];
         
        my $hreq = encode_json  $hostreq;
        my $resp = $ua->request(POST $baseurl . "81/performance/Host/metrics", Content_Type => 'application/json',  Content => $hreq);
        if ($resp->is_success) {
          my $cleanhost=$hostid;
          $cleanhost =~ tr/\.\ /__/ ;
          my $res=decode_json $resp->content();
          my $ra=$res->{'resultList'}->{'result'};
          foreach my $resvals (@$ra) {
            if (defined $resvals) {
              my $ts=int($resvals->{'timestamp'}/1000);
              foreach my $rk (keys %$resvals) {
                if ($rk ne 'timestamp') {
                  my $val=$resvals->{$rk} + 0;
                  print $sock "$prefix.$sym.host.$cleanhost.$rk $val $ts\n";
                  # print       "$prefix.$sym.host.$cleanhost.$rk $val $ts\n";
                }  
              }
            }    
          }
          # print Dumper($ra),"\n";
        } else {
          print "FAILED host request $symid $hostid ",$resp->status_line," $hreq\n";
        }  

        # print "$symid $hostid\n";
     }
     
  } else {
     # print "=" x 20 , " failed $symid error: ",$response->status_line,"\n";
  } 
  
  my $sreq;
  # $sreq->{'startDate'}=($last-600)*1000;
  # $sreq->{'endDate'}=$last*1000;
  $sreq->{'symmetrixId'}=$symid;
  $req = encode_json  $sreq; 
  print "requesting storage group metrics for  $symid at " . localtime() . "\n";
  
  $response = $ua->request(POST $baseurl . "performance/StorageGroup/keys", Content_Type => 'application/json',  Content => $req);
  # print $response->content();
  if ($response->is_success) {
     my $res=decode_json $response->content();
     #print  Dumper($res),"\n";
     my $sghosts=$res->{'storageGroupInfo'};
     foreach my $k (@$sghosts) {
       my $hostid=$k->{'storageGroupId'}; 
       # print "doing $hostid\n";
        my $hostreq;
        $hostreq->{'startDate'}=($last-300)*1000;
        $hostreq->{'endDate'}=$last*1000-1;
        $hostreq->{'symmetrixId'}=$symid;
        $hostreq->{'storageGroupId'}=$hostid;
        $hostreq->{'metrics'}=['ReadResponseTime','WriteResponseTime','HostWrites','HostReads','HostMBs','HostMBReads','HostMBWritten','AvgReadSize','AvgWriteSize','BlockSize','BEMBReads','BEMBWritten'];
         
        my $hreq = encode_json  $hostreq;
        
        my $resp = $ua->request(POST $baseurl . "performance/StorageGroup/metrics", Content_Type => 'application/json',  Content => $hreq);
        #print  Dumper($resp),"\n";
        
        if ($resp->is_success) {
          my $cleanhost=$hostid;
          $cleanhost =~ tr/\.\ /__/ ;
          my $res=decode_json $resp->content();
          my $ra=$res->{'resultList'}->{'result'};
          foreach my $resvals (@$ra) {
            if (defined $resvals) {
              my $ts=int($resvals->{'timestamp'}/1000);
              foreach my $rk (keys %$resvals) {
                if ($rk ne 'timestamp') {
                  my $val=$resvals->{$rk} + 0;
                  print $sock "$prefix.$sym.storagegroup.$cleanhost.$rk $val $ts\n";
                  # print       "$prefix.$sym.storagegroup.$cleanhost.$rk $val $ts\n";
                }  
              }
            }    
          }
          # print Dumper($ra),"\n";
        } else {
          print "FAILED host request $symid $hostid ",$resp->status_line," $hreq\n";
        }  

        # print "$symid $hostid\n";
     }
     
  } else {
     # print "=" x 20 , " failed $symid error: ",$response->status_line,"\n";
  } 
  my $freq;
  # $sreq->{'startDate'}=($last-600)*1000;
  # $sreq->{'endDate'}=$last*1000;
  $freq->{'symmetrixId'}=$symid;
  $req = encode_json  $sreq; 
  print "requesting fedirector for $symid at " . localtime() . "\n";  
  $response = $ua->request(POST $baseurl . "performance/FEDirector/keys", Content_Type => 'application/json',  Content => $req);
  # print $response->content();
  if ($response->is_success) {
     my $res=decode_json $response->content();
     # print  Dumper($res),"\n";
     my $feds=$res->{'feDirectorInfo'};
     # print  Dumper($feds),"\n";
     foreach my $fed (@$feds) {
       # print Dumper($fed),"\n";
       my $dirid=$fed->{'directorId'};
       # print "got $dirid for $symid\n";
       my $dirreq;
       $dirreq->{'startDate'}=($last-300)*1000;
       $dirreq->{'endDate'}=$last*1000-1;
       $dirreq->{'symmetrixId'}=$symid;
       $dirreq->{'directorId'}=$dirid;
       $dirreq->{'metrics'}=['PercentBusy','HostIOs','ReadReqs','WriteReqs','ReadResponseTime','WriteResponseTime','HostMBs','ReadMissReqs','WriteMissReqs','QueueDepthUtilization'];
         
       my $hreq = encode_json  $dirreq;
       my $resp = $ua->request(POST $baseurl . "performance/FEDirector/metrics", Content_Type => 'application/json',  Content => $hreq);
       if ($resp->is_success) {
         my $cleandir=$dirid;
         $cleandir =~ tr/\.\ /__/ ;
         my $res=decode_json $resp->content();
         # print  Dumper($res),"\n";
         my $ra=$res->{'resultList'}->{'result'};
         foreach my $resvals (@$ra) {
           if (defined $resvals) {
             my $ts=int($resvals->{'timestamp'}/1000);
             foreach my $rk (keys %$resvals) {
               if ($rk ne 'timestamp') {
                 my $val=$resvals->{$rk} + 0;
                  print $sock "$prefix.$sym.fedirector.$cleandir.$rk $val $ts\n";
                  # print "$prefix.$sym.fedirector.$cleandir.$rk $val $ts\n";
               }  
             }
           }    
         }
       } else {
         print "request for $dirid failed with: ",$resp->status_line,"\n\n";
       }
    }    
  } else {
     print "=" x 20 , " failed $symid error: ",$response->status_line,"\n";
  } 


}

print "-" x 40 ," removing lock at " . localtime() . "\n";
unlink("v2g.lck");


$sock->shutdown(2);

                                      