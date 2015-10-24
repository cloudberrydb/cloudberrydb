CREATE OR REPLACE FUNCTION plperl_sum_array(INTEGER[]) RETURNS text AS $$
	my $array_arg = shift;
	my $result = 0;
	my @arrays;

	push @arrays, @$array_arg;

	while (@arrays > 0) {
		my $el = shift @arrays;
		if (is_array_ref($el)) {
			push @arrays, @$el;
		} else {
			$result += $el;
		}
	}
	return $result.' '.$array_arg;
$$ LANGUAGE plperl;

select plperl_sum_array('{1,2,NULL}');
select plperl_sum_array('{}');
select plperl_sum_array('{{1,2,3}, {4,5,6}}');
select plperl_sum_array('{{{1,2,3}, {4,5,6}}, {{7,8,9}, {10,11,12}}}');

-- check whether we can handle arrays of maximum dimension (6)
select plperl_sum_array(ARRAY[[[[[[1,2],[3,4]],[[5,6],[7,8]]],[[[9,10],[11,12]],
[[13,14],[15,16]]]],
[[[[17,18],[19,20]],[[21,22],[23,24]]],[[[25,26],[27,28]],[[29,30],[31,32]]]]],
[[[[[1,2],[3,4]],[[5,6],[7,8]]],[[[9,10],[11,12]],[[13,14],[15,16]]]],
[[[[17,18],[19,20]],[[21,22],[23,24]]],[[[25,26],[27,28]],[[29,30],[31,32]]]]]]);

-- what would we do with the arrays exceeding maximum dimension (7)
select plperl_sum_array('{{{{{{{1,2},{3,4}},{{5,6},{7,8}}},{{{9,10},{11,12}},
{{13,14},{15,16}}}},
{{{{17,18},{19,20}},{{21,22},{23,24}}},{{{25,26},{27,28}},{{29,30},{31,32}}}}},
{{{{{1,2},{3,4}},{{5,6},{7,8}}},{{{9,10},{11,12}},{{13,14},{15,16}}}},
{{{{17,18},{19,20}},{{21,22},{23,24}}},{{{25,26},{27,28}},{{29,30},{31,32}}}}}},
{{{{{{1,2},{3,4}},{{5,6},{7,8}}},{{{9,10},{11,12}},{{13,14},{15,16}}}},
{{{{17,18},{19,20}},{{21,22},{23,24}}},{{{25,26},{27,28}},{{29,30},{31,32}}}}},
{{{{{1,2},{3,4}},{{5,6},{7,8}}},{{{9,10},{11,12}},{{13,14},{15,16}}}},
{{{{17,18},{19,20}},{{21,22},{23,24}}},{{{25,26},{27,28}},{{29,30},{31,32}}}}}}}'
);

select plperl_sum_array('{{{1,2,3}, {4,5,6,7}}, {{7,8,9}, {10, 11, 12}}}');

CREATE OR REPLACE FUNCTION plperl_concat(TEXT[]) RETURNS TEXT AS $$
	my $array_arg = shift;
	my $result = "";
	my @arrays;
	
	push @arrays, @$array_arg;
	while (@arrays > 0) {
		my $el = shift @arrays;
		if (is_array_ref($el)) {
			push @arrays, @$el;
		} else {
			$result .= $el;
		}
	}
	return $result.' '.$array_arg;
$$ LANGUAGE plperl;

select plperl_concat('{"NULL","NULL","NULL''"}');
select plperl_concat('{{NULL,NULL,NULL}}');
select plperl_concat('{"hello"," ","world!"}');

-- composite type containing arrays
CREATE TYPE rowfoo AS (bar INTEGER, baz INTEGER[]);

CREATE OR REPLACE FUNCTION plperl_sum_row_elements(rowfoo) RETURNS TEXT AS $$
	my $row_ref = shift;
	my $result;
	
	if (ref $row_ref ne 'HASH') {
		$result = 0;
	}
	else {
		$result = $row_ref->{bar};
		die "not an array reference".ref ($row_ref->{baz}) 
		unless (is_array_ref($row_ref->{baz}));
		# process a single-dimensional array
		foreach my $elem (@{$row_ref->{baz}}) {
			$result += $elem unless ref $elem;
		}
	}
	return $result;
$$ LANGUAGE plperl;

select plperl_sum_row_elements(ROW(1, ARRAY[2,3,4,5,6,7,8,9,10])::rowfoo);

-- check arrays as out parameters
CREATE OR REPLACE FUNCTION plperl_arrays_out(OUT INTEGER[]) AS $$
	return [[1,2,3],[4,5,6]];
$$ LANGUAGE plperl;

select plperl_arrays_out();

-- check that we can return the array we passed in
CREATE OR REPLACE FUNCTION plperl_arrays_inout(INTEGER[]) returns INTEGER[] AS $$
	return shift;
$$ LANGUAGE plperl;

select plperl_arrays_inout('{{1}, {2}, {3}}');

-- make sure setof works
create or replace function perl_setof_array(integer[]) returns setof integer[] language plperl as $$
	my $arr = shift;
	for my $r (@$arr) {
		return_next $r;
	}
	return undef;
$$;

select perl_setof_array('{{1}, {2}, {3}}');
