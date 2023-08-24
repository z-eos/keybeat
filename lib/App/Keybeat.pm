#!/usr/local/bin/perl
# -*- mode: cperl; eval: (follow-mode 1); cperl-indent-level: 2; cperl-continued-statement-offset: 2 -*-
#

package App::Keybeat;

use strict;
use warnings;
use diagnostics;

use Carp;
use DBI;
# use Data::Printer caller_info => 1, colored => 1, print_escapes => 1, output => 'stdout', class => { expand => 2 },
#   caller_message => "DEBUG __FILENAME__:__LINE__ ";
use Data::Printer;
use File::Basename;
use File::stat;
use File::Tail;
use Getopt::Long  qw(:config no_ignore_case gnu_getopt auto_help auto_version);
use Net::LDAP;
use Parse::Syslog::Mail;
use Pod::Man;
use Pod::Usage qw(pod2usage);;
use POSIX;
use Sys::Syslog qw(:standard :extended :macros);
use Time::Piece;

use App::Keybeat::Logg;

my  @PROGARG = ($0, @ARGV);
our $VERSION = '0.0.1';

sub new {
  my $class = shift;
  my $tl = localtime;
  my $self =
    bless {
	   _progname => fileparse($0),
	   _progargs => [$0, @ARGV],
	   _daemonargs => [$0, @ARGV],
	   _opt => {
		    colored           => 0,
		    db  => { name => undef, },
		    log => {
			    logfile => '/var/log/maillog',
			    save_to => '',
			   },
		    syslog => {
			       facility => 'LOG_USER',
			      },
		    relay_domains_sfx => [ 'root','xx','ibs','ibs.dn.ua' ],
		    v                 => 0,
		    daemon            => 0,
		    dryrun            => 0,
		    count             => 1,
		    export            => 'raw',
		    last_forever      => 1,
		    tail              => 0,
		    ts_fmt            => "%a %F %T %Z (%z)",
		   },
	   _tl       => $tl,
	  }, $class;

  GetOptions(
             'l|logfile=s' => \$self->{_opt}{log}{logfile},
             's|save-to=s' => \$self->{_opt}{log}{save_to},
	     'v|verbose+'  => \$self->{_opt}{v},
	     'c'           => \$self->{_opt}{count},
	     'D|daemon'    => \$self->{_opt}{daemon},
	     'd|db=s'      => \$self->{_opt}{db}{name},
	     'e|export=s'  => \$self->{_opt}{export},
	     'colors'      => \$self->{_opt}{colors},
	     'fg'          => \$self->{_opt}{fg},
	     'n|dry-run'   => \$self->{_opt}{dryrun},
	     't|tail'      => \$self->{_opt}{tail},

	     'h|help'              => sub { pod2usage(-exitval => 0, -verbose => 2); exit 0 },
	     'V|version'           => sub { print "$self->{_progname}, version $VERSION\n"; exit 0 },
	    );

  $self->{_opt}{l} = new
    App::Keybeat::Logg( prognam    => $self->{_progname},
			 facility   => $self->{_opt}{syslog}{facility},
			 foreground => $self->{_opt}{fg},
			 colors     => $self->{_opt}{colors} );

  if ( ! -f $self->{_opt}{log}{logfile} ) {
    $self->l->cc( pr => 'err', fm => "%s:%s: log file: %s; %m",
		  ls => [ __FILE__,__LINE__,$self->{log}->{save_to} ] );
    exit 1;
  } elsif ( defined $self->{log}->{save_to} && ! -d $self->{log}->{save_to} ) {
    $self->l->cc( pr => 'err', fm => "%s:%s: dir to save db file: %s; %m",
		  ls => [ __FILE__,__LINE__,$self->{log}->{save_to} ] );
    exit 1;
  }

  ( $self->{_opt}{log}{name}, $self->{_opt}{log}{dirs}, $self->{_opt}{log}{suffix} ) = fileparse($self->o('log')->{logfile});
  $self->{_opt}{log}{stat} = stat($self->o('log')->{logfile});

  if ( $self->{_opt}{export} && 
       $self->{_opt}{export} ne 'sqlite' &&
       $self->{_opt}{export} ne 'raw' ) {
    $self->l->cc( pr => 'err', fm => "%s:%s: Wrong export format.",
		  ls => [ __FILE__,__LINE__ ] );
    exit 1;
  } elsif ( $self->{_opt}{export} eq 'sqlite' ) {
    if ( ! defined $self->{_opt}{db}{name} ) {
      $self->{_opt}{db}{name} =
	sprintf('%s%s-%s-v%s%s.sqlite',
		$self->{_opt}{log}{save_to} ne '' ? $self->{_opt}{log}{save_to} . '/' : $self->{_opt}{log}{dirs},
		$self->{_opt}{log}{name},
		localtime($self->{_opt}{log}{stat}->mtime)->ymd(''),
		$self->{_tl}->ymd(''),
		$self->{_tl}->hms(''),
	       );
    }
  }
  $self->l->cc( pr => 'info', fm => "%s:%s: options: %s",
		ls => [ __FILE__,__LINE__, $self->{_opt} ] )
    if $self->{_opt}{v} > 2;

  $self->l->cc( pr => 'info', fm => "%s:%s: log file to be used is: %s",
		ls => [ __FILE__,__LINE__, $self->{_opt}{log}{logfile} ] )
    if $self->{_opt}{v};

  return $self;
}

sub progname { shift->{_progname} }
sub progargs { return join(' ', @{shift->{_progargs}}); }

sub o {
  my ($self,$opt) = @_;
  croak "unknown/undefined variable"
    if ! exists $self->{_opt}{$opt};
  return $self->{_opt}{$opt};
}

sub l { shift->{_opt}{l} }

sub v { shift->{_opt}{v} }

sub run {
  my $self = shift;

  my $res;
  my $file;
  if ( $self->o('tail') ) {
    $file = File::Tail->new($self->o('log')->{logfile});
  } else {
    $file = $self->o('log')->{logfile};
  }
  my $maillog = Parse::Syslog::Mail->
    new( $self->o('tail') ? File::Tail->new($self->o('log')->{logfile}) : 
	 $self->o('log')->{logfile},
	 allow_future => 1);

  $self->daemonize if ! $self->o('fg');

  $self->sql_db_create
    if $self->o('export') eq 'sqlite' &&
    ! -e $self->o('db')->{name};

  while ( $self->o('last_forever') ) {
    while ( my $r = $maillog->next ) {
      next if exists $r->{ldap};
      next if $r->{text} =~ /AUTH|STARTTLS|--|NOQUEUE/;

      if ( exists $r->{'to'} ) {

	$res->{$r->{id}}->{timestamp}->{to} = $r->{timestamp}         // 'NA';
	$res->{$r->{id}}->{delay}           = $r->{delay}             // 'NA';
	$res->{$r->{id}}->{xdelay}          = $r->{xdelay}            // 'NA';
	$res->{$r->{id}}->{dsn}             = $r->{dsn}               // 'NA';
	$res->{$r->{id}}->{status}          = $r->{status}            // 'NA';
	$res->{$r->{id}}->{addr}->{to}      = $self->strip_addr($r->{to})
	  if exists $r->{to};
	$res->{$r->{id}}->{relay}->{to}     = $self->split_relay($r->{relay})
	  if exists $r->{relay};

	$self->l->cc( pr => 'info', fm => "%s:%s: rcpt_to: %s",
		      ls => [ __FILE__,__LINE__, $res->{$r->{id}} ] )
	  if $self->v > 3;

	$self->
	  sql_insert({
		      table => 'rcpt_to',
		      values =>
		      [
		       $r->{id} // undef,
		       $r->{timestamp} // undef,
		       $res->{$r->{id}}->{addr}->{to} // undef,
		       $res->{$r->{id}}->{delay} // undef,
		       $res->{$r->{id}}->{xdelay} // undef,
		       $res->{$r->{id}}->{relay}->{to}->{ip} // undef,
		       $res->{$r->{id}}->{relay}->{to}->{fqdn} // undef,
		       $res->{$r->{id}}->{dsn} // undef,
		       $res->{$r->{id}}->{status} // undef,
		      ]
		     });
      
      } elsif ( exists $r->{'from'} ) {

	$res->{$r->{id}}->{timestamp}->{fr} = $r->{timestamp}         // 'NA';
	$res->{$r->{id}}->{size}            = $r->{size};
	$res->{$r->{id}}->{addr}->{fr}      = $self->strip_addr($r->{from})
	  if exists $r->{from};
	$res->{$r->{id}}->{msgid}           = $self->strip_addr($r->{msgid})
	  if exists $r->{msgid};
	$res->{$r->{id}}->{relay}->{fr}     = $self->split_relay($r->{relay})
	  if exists $r->{relay};

	$self->l->cc( pr => 'info', fm => "%s:%s: mail_from: %s",
		      ls => [ __FILE__,__LINE__, $res->{$r->{id}} ] )
	  if $self->v > 3;

	$self->
	  sql_insert({
		      table => 'mail_from',
		      values =>
		      [
		       $r->{id} // undef,
		       $r->{timestamp} // undef,
		       $res->{$r->{id}}->{addr}->{fr} // undef,
		       $r->{size} // undef,
		       $res->{$r->{id}}->{relay}->{fr}->{ip} // undef,
		       $res->{$r->{id}}->{relay}->{fr}->{fqdn} // undef,
		       $res->{$r->{id}}->{msgid} // undef,
		      ]
		     });
      }

      $res->{$r->{id}}->{connection} = $r->{status}
	if exists $r->{status} &&
	$r->{status} =~ /^.*connection.*$/ &&
	$r->{status} !~ /^.*did not issue.*$/;

      delete $res->{$r->{id}} if $self->o('tail');
    }

    $self->l->cc( pr => 'info', fm => "%s:%s: res: %s",
		  ls => [ __FILE__,__LINE__, $res ] )
      if $self->o('export') eq 'raw' && $self->v > 2;

    if ($self->o('export') eq 'sqlite' && ! $self->o('tail')) {
      $self->sql_db_create if ! exists $self->o('db')->{name};
      $self->tosqlite( { log_rows  => $res, } );
    }

    closelog();
    $self->{_opt}{last_forever} = 0 if ! $self->o('daemon');
  }
}

######################################################################
#
######################################################################

sub daemonize {
  my $self = shift;
  my $pidfile = '/var/run/keybeat.pid';
  my ( $pid, $fh, $pp, $orphaned_pid_mtime );
  if ( -e $pidfile ) {
    open( $fh, "<", $pidfile) || do {
      die "Can't open $pidfile for reading: $!";
      exit 1;
    };
    $pid = <$fh>;
    close($fh) || do { print "closing $pidfile failed: $!\n\n";
		       exit 1;
		     };

    if ( kill(0, $pid) ) {
      print "Doing nothing\npidfile $pidfile of proces with pid $pid, exists and the process is alive\n\n";
      exit 1;
    }

    $orphaned_pid_mtime = strftime( $self->o('ts_fmt'), localtime( (stat( $pidfile ))[9] ));
    if ( unlink $pidfile ) {
      $self->l->cc( pr => 'debug', fm => "%s:%s: orphaned %s was removed",
		    ls => [ __FILE__,__LINE__, $pidfile ] )
	if $self->o('v') > 0;
    } else {
      $self->l->cc( pr => 'err', fm => "%s:%s: orphaned %s (mtime: %s) was not removed: %s",
		    ls => [ __FILE__,__LINE__, $pidfile, $orphaned_pid_mtime, $! ] );
      exit 2;
    }

    undef $pid;
  }

  $pid = fork();
  die "fork went wrong: $!\n\n" unless defined $pid;
  exit(0) if $pid != 0;

  setsid || do { print "setsid went wrong: $!\n\n"; exit 1; };

  open( $pp, ">", $pidfile) || do {
    print "Can't open $pidfile for writing: $!"; exit 1; };
  print $pp "$$";
  close( $pp ) || do {
    print "close $pidfile (opened for writing), failed: $!\n\n"; exit 1; };

  if ( $self->o('v') > 1 ) {
    open (STDIN,  "</dev/null") || do { print "Can't redirect /dev/null to STDIN\n\n";  exit 1; };
    open (STDOUT, ">/dev/null") || do { print "Can't redirect STDOUT to /dev/null\n\n"; exit 1; };
    open (STDERR, ">&STDOUT")   || do { print "Can't redirect STDERR to STDOUT\n\n";    exit 1; };
  }

  $SIG{HUP}  =
    sub { my $sig = @_;
	  $self->l->cc( pr => 'warning', fm => "%s:%s: SIG %s received, restarting", ls => [ __FILE__,__LINE__, $sig ] );
	  exec('perl', @{$self->o('_daemonargs')}); };
  $SIG{INT} = $SIG{QUIT} = $SIG{ABRT} = $SIG{TERM} =
    sub { my $sig = @_;
	  $self->l->cc( pr => 'warning',ls => [ __FILE__,__LINE__, $sig ],
			fm => "%s:%s:  SIG %s received, exiting" );
	  $self->{_opt}{last_forever} = 0;
	};
  $SIG{PIPE} = 'ignore';
  $SIG{USR1} =
    sub { my $sig = @_;
	  $self->l->cc( pr => 'warning',ls => [ __FILE__,__LINE__, $sig ],
			fm => "%s:%s: SIG %s received, doing nothing" ) };

  $self->l->cc( pr => 'info', fm => "%s:%s: %s v.%s is started.",
		ls => [ __FILE__,__LINE__, $self->progname, $VERSION ] );
}

sub sql_insert {
  my ($self, $args) = @_;

  my $dbh = DBI->connect("dbi:SQLite:dbname=" . $self->o('db')->{name},"","",
			 { AutoCommit => 1, RaiseError => 1, });

  $dbh->do("PRAGMA cache_size = 100000") or die $dbh->errstr;
  $dbh->begin_work or die $dbh->errstr;

  my $q =   'INSERT OR IGNORE INTO ' . $args->{table} . ' VALUES (';
  if ( $args->{table} eq 'rcpt_to' ) {
    $q .= '?,?,?,?,?,?,?,?,?)';
  } elsif ( $args->{table} eq 'mail_from' ) {
    $q .= '?,?,?,?,?,?,?)';
  }

  my $sth = $dbh->prepare($q);
  $sth->execute( @{$args->{values}} );

  $dbh->commit or die $dbh->errstr;
  $dbh->disconnect;
}

sub sql_db_create {
  my ($self, $args) = @_;

  $self->l->cc( pr => 'info', fm => "%s:%s: db file to be created is: %s",
		ls => [ __FILE__,__LINE__, $self->o('db')->{name} ] )
    if $self->v > 1;
  # print "database file to be created is: ",$self->o('db')->{name},"\n" if $self->v > 1;

  my $dbh = DBI->connect("dbi:SQLite:dbname=" . $self->o('db')->{name},"","",
			   { AutoCommit => 1,
			     RaiseError => 1, });
  $dbh->do("PRAGMA cache_size = 100000") or die $dbh->errstr;
  $dbh->begin_work or die $dbh->errstr;

  my $stub1 = $self->o('log')->{logfile};
  my $stub2 = $self->o('db')->{name};
  my $tbl_create = qq{CREATE TABLE maillog
  -- $stub1 data processed, generated with mailstate
  -- results are written to $stub2
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
  $self->l->cc( pr => 'info', fm => "%s:%s: tbl_create: %s",
		ls => [ __FILE__,__LINE__, $tbl_create ] )
    if $self->v > 2;
  $dbh->do($tbl_create) or die $dbh->errstr;

  $tbl_create = qq{CREATE TABLE IF NOT EXISTS addr_to_unique
  -- $stub1 data processed, generated with mailstate
  -- results are written to $stub2
  -- table contains all unique, not served by us, RCPT TO addresses 
  (
    addr_to_unique TEXT PRIMARY KEY, -- RCPT TO (not ours recipients)
    addr_to_count  NUM               -- emails sent to addr_to number
  );};
  $self->l->cc( pr => 'info', fm => "%s:%s: tbl_create: %s",
		ls => [ __FILE__,__LINE__, $tbl_create ] )
    if $self->v > 2;
  $dbh->do($tbl_create) or die $dbh->errstr;

  $tbl_create = qq{CREATE TABLE rcpt_to
  -- $stub1 data processed, generated with mailstate
  -- results are written to $stub2
  (
    id              TEXT, -- sendmail message ID (macros \$i)
    ts              TEXT, -- timestamp
    addr_to         TEXT, -- RCPT To
    delay           TEXT, -- delay
    xdelay          TEXT, -- delay
    relay_to_ip     TEXT, -- ip address of the recipient relay
    relay_to_fqdn   TEXT, -- fqdn of the recipient relay
    dsn             TEXT, -- DSN code
    stat            TEXT  -- Status
  );};
  $self->l->cc( pr => 'info', fm => "%s:%s: tbl_create: %s",
		ls => [ __FILE__,__LINE__, $tbl_create ] )
    if $self->v > 2;
  $dbh->do($tbl_create) or die $dbh->errstr;

  $tbl_create = qq{CREATE TABLE mail_from
  -- $stub1 data processed, generated with mailstate
  -- results are written to $stub2
  (
    id              TEXT, -- sendmail message ID (macros \$i)
    ts              TEXT, -- timestamp
    addr_fr         TEXT, -- MAIL From 
    size            NUM,  -- message size
    relay_fr_ip     TEXT, -- ip address of the sender relay
    relay_fr_fqdn   TEXT, -- fqdn of the sender relay
    msgid           TEXT  -- Message-ID header
  );};
  $self->l->cc( pr => 'info', fm => "%s:%s: tbl_create: %s",
		ls => [ __FILE__,__LINE__, $tbl_create ] )
    if $self->v > 2;
  $dbh->do($tbl_create) or die $dbh->errstr;

  my $idx_create =
    [
     q{CREATE INDEX m_addr_fr       ON maillog ( addr_fr );},
     q{CREATE INDEX m_addr_to       ON maillog ( addr_to );},
     q{CREATE INDEX m_from_to_addr  ON maillog ( addr_fr, addr_to );},
     q{CREATE INDEX m_relay_fr_ip   ON maillog ( relay_fr_ip );},
     q{CREATE INDEX m_relay_fr_fqdn ON maillog ( relay_fr_fqdn );},
     q{CREATE INDEX m_relay_to_ip   ON maillog ( relay_to_ip );},
     q{CREATE INDEX m_relay_to_fqdn ON maillog ( relay_to_fqdn );},
     q{CREATE INDEX m_msgid         ON maillog ( msgid );},
     q{CREATE INDEX m_stat          ON maillog ( stat );},

     q{CREATE INDEX f_addr_fr       ON mail_from ( addr_fr );},
     q{CREATE INDEX t_id_stat       ON rcpt_to   ( id, stat );},
     q{CREATE INDEX t_addr_to       ON rcpt_to   ( addr_to );},
     q{CREATE INDEX f_relay_fr_ip   ON mail_from ( relay_fr_ip );},
     q{CREATE INDEX f_relay_fr_fqdn ON mail_from ( relay_fr_fqdn );},
     q{CREATE INDEX t_relay_to_ip   ON rcpt_to   ( relay_to_ip );},
     q{CREATE INDEX t_relay_to_fqdn ON rcpt_to   ( relay_to_fqdn );},
    ];

  foreach (@{$idx_create}) {
    $self->l->cc( pr => 'info', fm => "%s:%s: idx_create: %s",
		  ls => [ __FILE__,__LINE__, $_ ] )
      if $self->v > 2;
    $dbh->do($_) or die $dbh->errstr;
  }

  $tbl_create = qq{CREATE VIEW view_maillog AS
    SELECT f.id AS id,
           f.ts AS ts,
           f.addr_fr AS addr_fr,
           t.addr_to AS addr_to,
           f.size AS size,
           t.delay AS delay,
           t.xdelay AS xdelay,
           f.relay_fr_ip AS relay_fr_ip,
           f.relay_fr_fqdn AS relay_fr_fqdn,
           t.relay_to_ip AS relay_to_ip,
           t.relay_to_fqdn AS relay_to_fqdn,
           t.dsn AS dsn,
           f.msgid AS msgid,
           t.stat AS stat
    FROM mail_from AS f LEFT JOIN rcpt_to AS t ON f.id = t.id
    ORDER BY f.ts, f.id, t.stat;};
  $self->l->cc( pr => 'info', fm => "%s:%s: tbl_create: %s",
		ls => [ __FILE__,__LINE__, $tbl_create ] )
    if $self->{_opt}{v} > 2;
  $dbh->do($tbl_create) or die $dbh->errstr;

  $dbh->commit or die $dbh->errstr;
  $dbh->disconnect;
}

sub relay_domains {
  my ($self, $args) = @_;
  #   my $args = shift @_;
#   my $relay_domains;
#   my $ldap = Net::LDAP->new ( 'localhost', async => 1 ) or die "$@";
#   my $bind_msg = $ldap->bind ( version => 3 );
#   p $bind_msg->error if $bind_msg->code;
#   my $fqdns =
#     $ldap->search(base   => "sendmailMTAMapName=smarttable,ou=relay.xx,ou=Sendmail,dc=ibs",
# 		  scope  => "one",
# 		  filter => "sendmailMTAKey=*",
# 		  attrs  => [ 'sendmailMTAKey' ],
# 		 );
#   my @fqdn_arr = $fqdns->entries;
#   my $a;
#   foreach ( @fqdn_arr ) {
#     $a = $_->get_value('sendmailMTAKey', asref => 1);
#     # p $a;
#     push @{$relay_domains}, substr($a->[0],1);
#   }
#   return $relay_domains;
  return [];
}

sub split_relay {
  my ($self, $relay) = @_;
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
  my ($self, $addr) = @_;
  $addr =~ tr/<>//d;
  return $addr;
}

sub tosqlite {
  my ($self, $args) = @_;

  $args->{logfilemtime} = localtime($self->o('log')->{stat}->mtime);

  my $tl = localtime;
  my $arg =
    {
     log_rows  => $args->{log_rows},
    };

  p $arg if $self->v > 3;

    $self->l->cc( pr => 'info', fm => "%s:%s: db file to be used is: %s",
		ls => [ __FILE__,__LINE__, $self->o('db')->{name} ] )
    if $self->{_opt}{v} > 1;

  my $dbh = DBI->connect("dbi:SQLite:dbname=" . $self->o('db')->{name},"","",
			 { AutoCommit => 1, RaiseError => 1, });

  $dbh->do("PRAGMA cache_size = 100000") or die $dbh->errstr;
  $dbh->begin_work or die $dbh->errstr;

  my $sth;
  foreach ( keys ( %{$arg->{log_rows}} ) ) {
    next if ! $arg->{log_rows}->{$_}->{addr}->{to} ||
      ! $arg->{log_rows}->{$_}->{addr}->{fr};
    $sth = $dbh->prepare('INSERT OR IGNORE INTO maillog VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)');
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

  $arg->{addr_to_unique_select} = sprintf("
INSERT OR IGNORE INTO addr_to_unique (addr_to_unique, addr_to_count)
       SELECT DISTINCT lower(addr_to), count(addr_to)
              FROM maillog WHERE substr(addr_to,instr(addr_to, '\@') + 1) NOT IN ( '%s' )
              AND addr_to NOT LIKE '%%,%%' GROUP BY addr_to",
					  join("','", @{relay_domains()}, @{$self->o('relay_domains_sfx')}));

  $self->l->cc( pr => 'info', fm => "%s:%s: addr_to_unique_select: %s",
		ls => [ __FILE__,__LINE__, $arg->{addr_to_unique_select} ] )
    if $self->v > 2;
  $dbh->do($arg->{addr_to_unique_select}) or die $dbh->errstr;
  #$sth = $dbh->prepare( $arg->{addr_to_unique_select} );
  #$sth->execute( "'" . join("','", @{relay_domains()}) . "','root'" );
  $dbh->commit or die $dbh->errstr;

  $dbh->disconnect;

  $self->l->cc( pr => 'info', fm => "%s:%s: processing %s -> %s complete",
		ls => [ __FILE__,__LINE__,$self->o('log')->{logfile},
			$self->o('db')->{name}, ] );
}

1;
