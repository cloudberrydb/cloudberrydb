#! /usr/bin/env python
import figleaf
from figleaf import internals
from sets import Set as set
import sys
from cPickle import load
import os
from optparse import OptionParser

def main():
    #### OPTIONS

    parser = OptionParser()

    parser.add_option('-c', '--coverage', nargs=1, action="store",
                      dest="coverage_file", 
                      help = 'load coverage info from this file',
                      default='.figleaf_sections')

    ####

    (options, args) = parser.parse_args(sys.argv[1:])
    coverage_file = options.coverage_file
    
    figleaf.load_pickled_coverage(open(coverage_file))
    data = internals.CoverageData(figleaf._t)
    full_cov = data.gather_files()

    for filename in args:
        annotate_file_with_sections(filename, data, full_cov)

def annotate_file_with_sections(short, data, full_cov):
    full = os.path.abspath(short)

    tags = {}
    sections = data.gather_sections(full)
    sections.update(data.gather_sections(short))

    print data.sections

    print '*** PROCESSING:', short, '\n\t==>', short + '.sections'
    for tag, cov in sections.items():
        if cov:
            tags[tag] = cov

    if not tags:
        print '*** No coverage info for file', short

    tag_names = tags.keys()
    tag_names.sort()
    tag_names.reverse()

    tags["-- all coverage --"] = full_cov.get(full, set())
    tag_names.insert(0, "-- all coverage --")

    n_tags = len(tag_names)
    
    fp = open(short + '.sections', 'w')

    for i, tag in enumerate(tag_names):
        fp.write('%s%s\n' % ('| ' * i, tag))
    fp.write('| ' * n_tags)
    fp.write('\n\n')

    source = open(full)
    for n, line in enumerate(source):
        marks = ""
        for tag in tag_names:
            cov = tags[tag]

            symbol = '  '
            if (n+1) in cov:
                symbol = '+ '

            marks += symbol

        fp.write('%s  | %s' % (marks, line))
    
    fp.close()
