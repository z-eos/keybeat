# -*- mode: cperl; eval: (follow-mode 1); cperl-indent-level: 2; cperl-continued-statement-offset: 2 -*-
use strict;
use warnings;
use Module::Metadata;
use ExtUtils::MakeMaker;

WriteMakefile(
	      NAME             => 'App::mailstate',
	      ABSTRACT         => 'script to get all "complete envelope" emails statistics from sendmail log file and to put all that data into sqlite db file',
	      VERSION_FROM     => 'lib/App/mailstate.pm',
	      EXE_FILES        => [ 'mailstate' ],
	      AUTHOR           => 'Zeus Panchenko <zeus@gnu.org.ua>',
	      LICENSE          => 'gpl_3',
	      MIN_PERL_VERSION => 5.016001,
	      PREREQ_PM        => {
				   'Data::Printer'        => '0.38',
				   'File::Basename'       => '2.84',
				   'Getopt::Long'         => '2.34',
				   'Parse::Syslog::Mail'  => '0.17',
				   'Pod::Man'             => '2.25',
				   'Pod::Usage'           => '1.51',
				   'Time::Piece'          => '1.05',
				  },

	      (eval
	       { ExtUtils::MakeMaker->VERSION(6.46) } ?
	       (META_MERGE => {
			       'meta-spec' => { version => 2 },
			       resources   => {
					       repository => {
							      type => 'git',
							      url  => 'ssh://git@github.com:z-eos/mailstate.git',
							      web  => 'https://github.com/z-eos/mailstate.git',
							     },
					      }})
	       : ()
	      ),

	     );
