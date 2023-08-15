#!/usr/local/bin/perl
# -*- mode: cperl; eval: (follow-mode 1); cperl-indent-level: 2; cperl-continued-statement-offset: 2 -*-
#

package App::mailstate;

use strict;
use warnings;
use diagnostics;

use Data::Printer caller_info => 1, colored => 1, print_escapes => 1, output => 'stdout', class => { expand => 2 },
  caller_message => "DEBUG __FILENAME__:__LINE__ ";
use File::Basename;
use File::stat;
use Getopt::Long  qw(:config no_ignore_case gnu_getopt auto_help auto_version);
use Net::LDAP;
use Parse::Syslog::Mail;
use Pod::Man;
use Pod::Usage qw(pod2usage);;
use Time::Piece;

my  @PROGARG = ($0, @ARGV);
our $VERSION = '0.0.1';

sub new {
  my $class = shift;
  my $self =
    bless {
	   _progname => fileparse($0),
	   _progargs => [$0, @ARGV],
	   _option   => { d                 => 0,
			  colored           => 0,
			  logfile           => '/var/log/maillog',
			  relay_domains_sfx => [ 'root','xx','ibs','ibs.dn.ua' ],
			  verbose           => 0,
			  dryrun            => 0,
			  count             => 1,
			  export            => '',
			  tl                => localtime,
			  
			},
	  }, $class;

  GetOptions(
             'l|logfile=s' => \$self->{_option}{logfile},
             's|save-to=s' => \$self->{_option}{ log->{saveto} },
	     'h|help'      => sub { pod2usage(0); exit 0 },
	     'v+'          => \$self->{_option}{verbose},
	     'c'           => \$self->{_option}{count},
	     'e|export=s'  => \$self->{_option}{export},
	     'n|dry-run'   => \$self->{_option}{dryrun},

	     'h|help'              => sub { pod2usage(-exitval => 0, -verbose => 2); exit 0 },
	     'd|debug+'            => \$self->{_option}{d},
	     'V|version'           => sub { print "$self->{_progname}, version $VERSION\n"; exit 0 },
	    );

  pod2usage(-exitval => 0, -verbose => 2, -msg => "\nERROR: Wrong export format.\n\n")
    if $self->{_option}{export} && 
       $self->{_option}{export} ne 'sqlite' &&
       $self->{_option}{export} ne 'raw';

  print "log file to be used is: $logfile\n" if $verbose > 0;

  pod2usage(-exitval => 0, -verbose => 2, -msg => "\nERROR: log file configured is $logfile; %m\n\n")
    if ! -f $self->{_option}{logfile};

  pod2usage(-exitval => 0, -verbose => 2, -msg => "\nERROR: no extension given, set it please.\n\n")
    if ! $self->{_option}{export};

  pod2usage(-exitval => 0, -verbose => 2, -msg => "\nERROR: no extension given, set it please.\n\n")
    if ( defined $log->{saveto} && ! -d $log->{saveto} ) {
    debug_msg( {priority => 'warning',
		message   => "warning: directory to save db file to is $log->{saveto}; %m",
		verbosity => $verbose });
    # pod2usage(0);
    exit 1;
  }

  return $self;
}

sub progname { shift->{_progname} }
sub progargs { return join(' ', @{shift->{_progargs}}); }

sub option {
  my ($self,$opt) = @_;
  return $self->{_option}{$opt};
}

# my $progname = 'mailstate';
# my $logfile = '/var/log/maillog';
# my $relay_domains_sfx = [ 'root','xx','ibs','ibs.dn.ua' ];
# our $verbose = 0;
# our $dryrun = 0;
# my $count = 1;
# my $export = '';
# my $tl = localtime;

my $log;
my @log_row;
my $res;
my $index;
my $element;
my $id;
my $ts;
my $t;
my $rest;
my $key;
my $val;
my $i;

$log->{saveto} = '';

  
$log->{logfile} = $logfile;
( $log->{name}, $log->{dirs}, $log->{suffix} ) = fileparse($logfile);
$log->{stat} = stat($logfile);

my $maillog = Parse::Syslog::Mail->new( $logfile,
				        allow_future => 1);
while( my $row = $maillog->next ) {
  next if $row->{text} =~ /AUTH|STARTTLS|--|NOQUEUE/;

  if ( exists $row->{'to'} ) {

    $res->{$row->{id}}->{timestamp}->{to} = $row->{timestamp}         // 'NA';
    $res->{$row->{id}}->{delay}           = $row->{delay}             // 'NA';
    $res->{$row->{id}}->{xdelay}          = $row->{xdelay}            // 'NA';
    $res->{$row->{id}}->{dsn}             = $row->{dsn}               // 'NA';
    $res->{$row->{id}}->{status}          = $row->{status}            // 'NA';
    $res->{$row->{id}}->{addr}->{to}      = strip_addr($row->{to})
      if exists $row->{to};
    $res->{$row->{id}}->{relay}->{to}     = split_relay($row->{relay})
      if exists $row->{relay};

  } elsif ( exists $row->{'from'} ) {

    $res->{$row->{id}}->{timestamp}->{fr} = $row->{timestamp}         // 'NA';
    $res->{$row->{id}}->{size}            = $row->{size};
    $res->{$row->{id}}->{addr}->{fr}      = strip_addr($row->{from})
      if exists $row->{from};
    $res->{$row->{id}}->{msgid}           = strip_addr($row->{msgid})
      if exists $row->{msgid};
    $res->{$row->{id}}->{relay}->{fr}     = split_relay($row->{relay})
      if exists $row->{relay};
  }

  $res->{$row->{id}}->{connection} = $row->{status}
    if exists $row->{status} &&
    $row->{status} =~ /^.*connection.*$/ &&
    $row->{status} !~ /^.*did not issue.*$/;
}

p $res if $export eq 'raw' || $verbose > 2;

tosqlite( { log_rows  => $res,
	    log       => $log,
	    localtime => $tl,
	  } )
  if $export eq 'sqlite' && ! $dryrun;

exit 0;

######################################################################
#
######################################################################

sub split_relay {
  my $relay = shift;
  my $return;
  if ( $relay =~ /^([\w,\.]+) \[(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\].*$/ ) {
    $return->{fqdn} = $1;
    $return->{ip}   = $2;
  } elsif ($relay =~ /\[(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\].*$/) {
    $return->{fqdn} = 'NA';
    $return->{ip}   = $1;
  } elsif ($relay =~ /\[(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\] \[(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\].*$/) {
    $return->{fqdn} = $1;
    $return->{ip}   = $2;
  } else {
    $return->{fqdn} = $relay;
    $return->{ip}   = 'NA';
  }
  return $return;
}

sub strip_addr {
  my $addr = shift;
  my $return;
  if ($addr =~ /<(.*@.*)>/) {
    $return = $1;
  } else {
    $return = $addr;
  }
  return lc($return);
}

sub tosqlite {
  my $args = shift @_;
  $args->{logfilemtime} = localtime($args->{log}->{stat}->mtime);

  my $arg = { log_rows  => $args->{log_rows},
	      log       => $args->{log},
	      localtime => $args->{localtime},
	      dbfile    =>
	      sprintf('%s%s-%s-v%s%s.sqlite',
		      $args->{log}->{saveto} ne '' ? $args->{log}->{saveto} . '/' : $args->{log}->{dirs},
		      $args->{log}->{name},
		      $args->{logfilemtime}->ymd(''),
		      $args->{localtime}->ymd(''),
		      $args->{localtime}->hms(''),
		     ),
	      verbose => $args->{verbose}, };

  p $arg if $verbose > 3;
  print "database file to be used is: $arg->{dbfile}\n" if $verbose;

  use DBI;

  my $dbh = DBI->connect("dbi:SQLite:dbname=$arg->{dbfile}","","",
			 { AutoCommit => 1,
			   RaiseError => 1, });

  $dbh->do("PRAGMA cache_size = 100000") or die $dbh->errstr;
  $dbh->begin_work or die $dbh->errstr;

  my $tbl_create = qq{CREATE TABLE maillog
  -- $arg->{log}->{logfile} data processed, generated with mailstate
  -- results are written to $arg->{dbfile}
  (
    id              TEXT PRIMARY KEY, -- sendmail message ID (macros \$i)
    ts              TEXT,             -- timestamp
    addr_fr         TEXT,             -- MAIL From
    addr_to         TEXT,             -- RCPT To
    size            NUM,              -- message size
    delay           TEXT,             -- delay
    xdelay          TEXT,             -- delay
    relay_fr_ip     TEXT,             -- ip address of the sender relay
    relay_fr_fqdn   TEXT,             -- fqdn of the sender relay
    relay_to_ip     TEXT,             -- ip address of the recipient relay
    relay_to_fqdn   TEXT,             -- fqdn of the recipient relay
    dsn             TEXT,             -- DSN code
    msgid           TEXT,             -- Message-ID header
    stat            TEXT              -- Status
  );};
  p $tbl_create if $verbose > 1;
  $dbh->do($tbl_create) or die $dbh->errstr;

  my $idx_create = [ q{CREATE INDEX addr_fr ON maillog ( addr_fr );},
		     q{CREATE INDEX addr_to ON maillog ( addr_to );},
		     q{CREATE INDEX from_to_addr ON maillog ( addr_fr, addr_to );},
		     q{CREATE INDEX relay_fr_ip ON maillog ( relay_fr_ip );},
		     q{CREATE INDEX relay_fr_fqdn ON maillog ( relay_fr_fqdn );},
		     q{CREATE INDEX relay_to_ip ON maillog ( relay_to_ip );},
		     q{CREATE INDEX relay_to_fqdn ON maillog ( relay_to_fqdn );},
		     q{CREATE INDEX msgid ON maillog ( msgid );},
		     q{CREATE INDEX stat ON maillog ( stat );} ];

  foreach (@{$idx_create}) {
    p $_ if $verbose > 1;
    $dbh->do($_) or die $dbh->errstr;
  }

  my $sth;
  foreach ( keys ( %{$arg->{log_rows}} ) ) {
    next if ! $arg->{log_rows}->{$_}->{addr}->{to} ||
      ! $arg->{log_rows}->{$_}->{addr}->{fr};
    $sth = $dbh->prepare('INSERT INTO maillog VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)');
    $sth->execute( $_,
		   $arg->{log_rows}->{$_}->{timestamp}->{to},
		   $arg->{log_rows}->{$_}->{addr}->{fr},
		   $arg->{log_rows}->{$_}->{addr}->{to},
		   $arg->{log_rows}->{$_}->{size},
		   $arg->{log_rows}->{$_}->{delay},
		   $arg->{log_rows}->{$_}->{xdelay},
		   $arg->{log_rows}->{$_}->{relay}->{fr}->{ip},
		   $arg->{log_rows}->{$_}->{relay}->{fr}->{fqdn},
		   $arg->{log_rows}->{$_}->{relay}->{to}->{ip},
		   $arg->{log_rows}->{$_}->{relay}->{to}->{fqdn},
		   $arg->{log_rows}->{$_}->{dsn},
		   $arg->{log_rows}->{$_}->{msgid},
		   $arg->{log_rows}->{$_}->{status},
		 );
  }

  $dbh->commit or die $dbh->errstr;

  $dbh->begin_work or die $dbh->errstr;
  $tbl_create = qq{CREATE TABLE IF NOT EXISTS addr_to_unique
  -- $arg->{log}->{logfile} data processed, generated with mailstate
  -- results are written to $arg->{dbfile}
  -- table contains all unique, not served by us, RCPT TO addresses 
  (
    addr_to_unique TEXT PRIMARY KEY, -- RCPT TO (not ours recipients)
    addr_to_count  NUM               -- emails sent to addr_to number
  );};
  p $tbl_create if $verbose > 1;
  $dbh->do($tbl_create) or die $dbh->errstr;

  $arg->{addr_to_unique_select} = sprintf("
INSERT INTO addr_to_unique (addr_to_unique, addr_to_count)
       SELECT DISTINCT lower(addr_to), count(addr_to)
              FROM maillog WHERE substr(addr_to,instr(addr_to, '\@') + 1) NOT IN ( '%s' )
              AND addr_to NOT LIKE '%%,%%' GROUP BY addr_to",
					  join("','", @{relay_domains()}, @{$relay_domains_sfx}));

  p $arg->{addr_to_unique_select} if $verbose > 1;
  $dbh->do($arg->{addr_to_unique_select}) or die $dbh->errstr;
  #$sth = $dbh->prepare( $arg->{addr_to_unique_select} );
  #$sth->execute( "'" . join("','", @{relay_domains()}) . "','root'" );
  $dbh->commit or die $dbh->errstr;

  $dbh->disconnect;

  debug_msg({ priority  => 'info',
	      message   => sprintf('info: processing complete %s%s -> %s',
				  $arg->{log}->{dirs},
				  $arg->{log}->{name},
				  $arg->{dbfile}),
	      verbosity => $verbose });
}

sub relay_domains {
  my $args = shift @_;
  my $relay_domains;
  my $ldap = Net::LDAP->new ( 'localhost', async => 1 ) or die "$@";
  my $bind_msg = $ldap->bind ( version => 3 );
  p $bind_msg->error if $bind_msg->code;
  my $fqdns =
    $ldap->search(base   => "sendmailMTAMapName=smarttable,ou=relay.xx,ou=Sendmail,dc=ibs",
		  scope  => "one",
		  filter => "sendmailMTAKey=*",
		  attrs  => [ 'sendmailMTAKey' ],
		 );
  my @fqdn_arr = $fqdns->entries;
  my $a;
  foreach ( @fqdn_arr ) {
    $a = $_->get_value('sendmailMTAKey', asref => 1);
    # p $a;
    push @{$relay_domains}, substr($a->[0],1);
  }
  return $relay_domains;
}
  
sub debug_msg {
  my $args = shift @_;
  my $arg = { priority => $args->{priority},
	      message => $args->{message},
	      verbosity => $args->{verbosity} || 0, };

  use Sys::Syslog qw(:standard :extended :macros);
  openlog($progname, "pid", LOG_MAIL);
  syslog($arg->{priority}, $arg->{message});
  closelog();
  print "DEBUG: $arg->{message}\n" if $arg->{verbosity} > 0
}

1;
