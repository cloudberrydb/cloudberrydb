#!/usr/bin/env python
import os
from collections import defaultdict

class prettyfloat(float):
	def __repr__(self):
		return '%6.2f' % self


if __name__ == '__main__':
	
	tests = []
	with open('regression.out', 'r') as f:
		for line in f:
			tokens=line.strip().split()
			t = tokens[4]
                        tests.append( (float(t[1:]), tokens[1]) )

	print '=============================='
	sorted_tests = sorted(tests)
	for t in sorted_tests:
		print '%6.2f' % t[0], 
		print t[1]

	test_set = set()
	[test_set.add(t[1]) for t in sorted_tests]
	runtimes = defaultdict(list)
	for v,k in sorted_tests:
		runtimes[k] = v
	print '=============================='
	#print runtimes
	#print runtimes['olap_group']

	with open('known_good_schedule', 'r') as f:
		with open('known_good_schedule.new', 'w') as fwr:
			for line in f:
				if not line.startswith('test:'):
					fwr.write(line)
					continue

				tokens = line.strip().split()
				if tokens[1] in test_set and runtimes[tokens[1]] > 1.0:
					fwr.write('ignore: '+tokens[1]+'\n')
				else:
					fwr.write(line)
	
	os.system("mv known_good_schedule.new known_good_schedule")
