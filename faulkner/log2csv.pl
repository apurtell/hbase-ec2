$last_rows = 0;
while (<>) {
    $line = $_;
    chomp $line;
    if ($line =~ /[0-9]+\/[0-9]+\ rows\ \@\ ([0-9]+)\ regions/) {
        $last_rows = $rows;
	$rows = $1;
    }
    if ($line =~ /([0-9]*\.[0-9]+|[0-9]+)\ ms/) {
	if ($last_rows != 0 && $last_rows != $rows) {
	    print "$last_rows\t" . $ms / $count . "\n";
	    $ms = 0;
	    $count = 0;
	}
	$ms = $ms + $1;
	$count++;
    }
}
print "$rows\t" . $ms / $count . "\n";
