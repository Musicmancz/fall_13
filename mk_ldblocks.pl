# Completed July 17
# Modified July 19 to add ability to directly populate ld_blocks table
# Modified starting 10 Sept to correct errors:
# Some SNPs added to multiple blocks
#
# Also want to add capability to recursively back-find paired SNPs

use strict;
use warnings;
#use DBI; use DBD::MySql;
use lib '/afs/isis/pkg/mysql/libperl-' .sprintf("%vd",$^V);
use DBD::mysql;

our $block_total; #Stores size of LD block associated with original RSID1 value
our @rsid_found; #Array storing the rsids found in an LD block, prevents infinite cycles of LD
our $fh; #File handler for outLDmap.txt
our $fh2; #File handler for totals.txt
our $dbh;
our %LD_idfind = {};
our $LD_number;
our $group;
our $fh_found;

sub findrs2 {
# Assigns given rsid to SNP1_rs to form query to find associated SNP2_rs entries.
# Runs recursively until no associated SNP2_rs found.

	my $rs1 = $_[0];
	my $chrom = $_[1];
	my $count = $_[2];

  	my $sth_2 = $dbh->prepare("
		SELECT SNP2_rs from LD 
		where SNP1_rs='$rs1' 
		AND chrom='$chrom' 
		AND population='$group'
		GROUP BY SNP2_rs
		"
	);
	$sth_2->execute();

	my @rs2; 
	my $rs;
	while ($rs = $sth_2->fetchrow ){
	
		push(@rs2 , $rs); #build array of SNP2 values associated with given SNP1
	 
	}				

	if ($#rs2 > 0) { 
    
		foreach $rs (@rs2) {
			next if exists $LD_idfind{$group}{$rs};

			if (`grep "$rs" found.txt`){
				next;
			}

			$block_total += 1;
	  		$LD_idfind{$group}{$rs} = $LD_number;

			print $fh_found "$rs\n";

			findrs2($rs,$chrom, $count+1);

    		}
	}
}

sub findrs1 {
	# Finds SNP1_rs entries linked to input rsid.
	# Works backwards to find linked SNPs
	
	my $rs2 = $_[0];
	my $chrom = $_[1];
	my $count = $_[2];

  	my $sth_1 = $dbh->prepare("
		SELECT SNP1_rs from LD 
		where SNP2_rs='$rs2' 
		AND population='$group'
		AND chrom='$chrom' 
		GROUP BY SNP1_rs
		"
	);
	$sth_1->execute();

	my @rs1; 
	my $rs;
	while ($rs = $sth_1->fetchrow ){
	
		push(@rs1 , $rs); #build array of SNP2 values associated with given SNP1
	 
	}				

	if ($#rs1 > 0) { 
    
		foreach $rs (@rs1) {
			next if exists $LD_idfind{$group}{$rs};

			if (`grep "$rs" found.txt`){
				next
			}
	  
			$block_total += 1;
	  		$LD_idfind{$group}{$rs} = $LD_number;
      		
			print $fh_found "$rs\n";

			findrs1($rs,$chrom, $count+1);

    		}
	}
}

sub uploadchr {

	my $population;

	for $population ( keys %LD_idfind) {

		while ( my ($key, $value) = each %{$LD_idfind{$population}}){
		
			my $sth_poschr = $dbh->prepare("
				SELECT chrom , SNP1_pos , SNP2_pos , SNP1_rs , SNP2_rs
				FROM ld
				WHERE SNP1_rs='$key'
				OR SNP2_rs='$key'
				LIMIT 1
				"
			);
			$sth_poschr->execute();

			my @results = $sth_poschr->fetchrow_array();

			my $sth_load=$dbh->prepare("
				INSERT INTO ld_blocks
				VALUES (?,?,?,?,?)
				"
			);

			if ($results[3] eq $key) {
				$sth_load->execute( 
					$results[3] , 
					$value , 
					$results[0] ,
					$results[1] ,
					$population
				);
			}
			else {
				$sth_load->execute( 
					$results[4] , 
					$value , 
					$results[0] ,
					$results[2] ,
					$population
				);
			}
		}
	}
}

$dbh=DBI->connect("DBI:mysql:database=czysz;host=152.2.15.164",'charlesczysz','CharlesSquared') or die "Could not connect to database.";


my $sth_chrom = $dbh->prepare("
	SELECT chrom 
	FROM LD 
	GROUP BY chrom
	"
);
$sth_chrom->execute();


my $sth_pop = $dbh->prepare("
	SELECT population
	FROM LD 
	GROUP BY population
   "
);
#$sth_pop->execute();


my @pop = ("CEU","YRI","CHB","JPT") ; # my $group;
#while ($group = $sth_pop->fetchrow) {
 # push(@pop , $group); #array of the different populations
#}

my @chrom;
my $chrom;
while ($chrom = $sth_chrom->fetchrow) {
	push(@chrom, $chrom);
}

my $sth_drop = $dbh->prepare("
	drop table if exists ld_blocks
	"
);
$sth_drop->execute();

my $sth_add = $dbh->prepare("
	create table ld_blocks (
	rsid varchar(255),
	ld_block int,
	chrom varchar(5),
	location int,
	population char(3))
	"
);
$sth_add->execute();

my @rs2;
foreach $chrom (@chrom) {

	%LD_idfind = {};

	foreach $group (@pop) {

		$LD_number = 0;

	    open $fh_found , ">found.txt";
		my $sth_rs1 = $dbh->prepare("
			SELECT SNP1_rs 
			FROM LD 
			WHERE chrom='$chrom' 
			AND population = '$group' 
			GROUP BY SNP1_rs
			"
		); 
		$sth_rs1->execute;
	  
		my $rs1;
		while ($rs1 = $sth_rs1->fetchrow){

			$block_total = 1;
			my $fcount = 0;
			my $bcount = 0;
			$LD_number++;
		  
			if (!(`grep "$rs1" found.txt`)){
	      
			findrs2($rs1 , $chrom , $fcount+1);
			findrs1($rs1, $chrom , $bcount+1);
	      
			}
	  	}
		close($fh_found);
	}
	uploadchr();
} 
 
