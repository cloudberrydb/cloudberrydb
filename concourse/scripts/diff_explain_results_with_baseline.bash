#!/bin/bash -l

function _main() {

	mkdir results/
	tar xf explain_output/*.tar.gz -C results/ out/ --strip-components 1

	mkdir baseline/
	tar xf explain_output_baseline/*.tar.gz -C baseline/ out/ --strip-components 1

	SUBEXPR='s/cost=[^ ]* //g; s/rows=[^ ]* //g; s/Time:.*$//g; s/Optimizer status:.*//g'
	ls results/* | while read fsql; do
		f=$(basename "$fsql")
		curdir=$(pwd)
		sed -e "$SUBEXPR" "$curdir/baseline/$f" > "/tmp/$f.baseline"
		sed -e "$SUBEXPR" "$curdir/results/$f"	> "/tmp/$f.results"
		diff -U3 -w "/tmp/$f.baseline" "/tmp/$f.results" >> simple.diffs

		echo "Filename: $f" >> results_combined.out
		cat "$curdir/results/$f" >> results_combined.out

		echo "Filename: $f" >> baseline_combined.out
		cat "$curdir/baseline/$f" >> baseline_combined.out
	done

	tar czvf explain_test_results.tar.gz results_combined.out baseline_combined.out
	cp explain_test_results.tar.gz diffs/
	cp simple.diffs diffs/

	echo "======================================================================"
	echo "DIFF FILE: simple.diffs"
	echo "======================================================================"
	cat diffs/simple.diffs

	echo
	echo "======================================================================"
	echo "SUMMARY"
	echo "======================================================================"
	gpdb_main_src/concourse/scripts/perfsummary.py --baseLog baseline_combined.out results_combined.out

	return $([ ! -s simple.diffs ]);
}

_main "$@"
