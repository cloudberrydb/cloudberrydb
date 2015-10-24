"""
figleaf is another tool to trace Python code coverage.

figleaf uses the sys.settrace hook to record which statements are
executed by the CPython interpreter; this record can then be saved
into a file, or otherwise communicated back to a reporting script.

figleaf differs from the gold standard of Python coverage tools
('coverage.py') in several ways.  First and foremost, figleaf uses the
same criterion for "interesting" lines of code as the sys.settrace
function, which obviates some of the complexity in coverage.py (but
does mean that your "loc" count goes down).  Second, figleaf does not
record code executed in the Python standard library, which results in
a significant speedup.  And third, the format in which the coverage
format is saved is very simple and easy to work with.

You might want to use figleaf if you're recording coverage from
multiple types of tests and need to aggregate the coverage in
interesting ways, and/or control when coverage is recorded.
coverage.py is a better choice for command-line execution, and its
reporting is a fair bit nicer.

Command line usage: ::

  figleaf <python file to execute> <args to python file>

The figleaf output is saved into the file '.figleaf', which is an
*aggregate* of coverage reports from all figleaf runs from this
directory.  '.figleaf' contains a pickled dictionary of sets; the keys
are source code filenames, and the sets contain all line numbers
executed by the Python interpreter. See the docs or command-line
programs in bin/ for more information.

High level API: ::

 * ``start(ignore_lib=True)`` -- start recording code coverage.
 * ``stop()``                 -- stop recording code coverage.
 * ``get_trace_obj()``        -- return the (singleton) trace object.
 * ``get_info()``             -- get the coverage dictionary

Classes & functions worth knowing about (lower level API):

 * ``get_lines(fp)`` -- return the set of interesting lines in the fp.
 * ``combine_coverage(d1, d2)`` -- combine coverage info from two dicts.
 * ``read_coverage(filename)`` -- load the coverage dictionary
 * ``write_coverage(filename)`` -- write the coverage out.
 * ``annotate_coverage(...)`` -- annotate a Python file with its coverage info.

Known problems:

 -- module docstrings are *covered* but not found.

AUTHOR: C. Titus Brown, titus@idyll.org, with contributions from Iain Lowe.

'figleaf' is Copyright (C) 2006, 2007 C. Titus Brown.  It is under the
BSD license.
"""
__version__ = "0.6.1"

# __all__ == @CTB

import sys
import os
from cPickle import dump, load
from optparse import OptionParser

import internals

# use builtin sets if in >= 2.4, otherwise use 'sets' module.
try:
    set()
except NameError:
    from sets import Set as set

def get_lines(fp):
    """
    Return the set of interesting lines in the source code read from
    this file handle.
    """
    # rstrip is a workaround for http://bugs.python.org/issue4262
    src = fp.read().rstrip() + "\n"
    code = compile(src, "", "exec")
    
    return internals.get_interesting_lines(code)

def combine_coverage(d1, d2):
    """
    Given two coverage dictionaries, combine the recorded coverage
    and return a new dictionary.
    """
    keys = set(d1.keys())
    keys.update(set(d2.keys()))

    new_d = {}
    for k in keys:
        v = d1.get(k, set())
        v2 = d2.get(k, set())

        s = set(v)
        s.update(v2)
        new_d[k] = s

    return new_d

def write_coverage(filename, append=True):
    """
    Write the current coverage info out to the given filename.  If
    'append' is false, destroy any previously recorded coverage info.
    """
    if _t is None:
        return

    data = internals.CoverageData(_t)

    d = data.gather_files()

    # sum existing coverage?
    if append:
        old = {}
        fp = None
        try:
            fp = open(filename)
        except IOError:
            pass

        if fp:
            old = load(fp)
            fp.close()
            d = combine_coverage(d, old)

    # ok, save.
    outfp = open(filename, 'w')
    try:
        dump(d, outfp)
    finally:
        outfp.close()

def read_coverage(filename):
    """
    Read a coverage dictionary in from the given file.
    """
    fp = open(filename)
    try:
        d = load(fp)
    finally:
        fp.close()

    return d

def dump_pickled_coverage(out_fp):
    """
    Dump coverage information in pickled format into the given file handle.
    """
    dump(_t, out_fp)

def load_pickled_coverage(in_fp):
    """
    Replace (overwrite) coverage information from the given file handle.
    """
    global _t
    _t = load(in_fp)

def annotate_coverage(in_fp, out_fp, covered, all_lines,
                      mark_possible_lines=False):
    """
    A simple example coverage annotator that outputs text.
    """
    for i, line in enumerate(in_fp):
        i = i + 1

        if i in covered:
            symbol = '>'
        elif i in all_lines:
            symbol = '!'
        else:
            symbol = ' '

        symbol2 = ''
        if mark_possible_lines:
            symbol2 = ' '
            if i in all_lines:
                symbol2 = '-'

        out_fp.write('%s%s %s' % (symbol, symbol2, line,))

def get_data():
    if _t:
        return internals.CoverageData(_t)

#######################

#
# singleton functions/top-level API
#

_t = None

def init(exclude_path=None, include_only=None):
    from internals import CodeTracer
    
    global _t
    if _t is None:
        _t = CodeTracer(exclude_path, include_only)

def start(ignore_python_lib=True):
    """
    Start tracing code coverage.  If 'ignore_python_lib' is True on
    initial call, ignore all files that live below the same directory as
    the 'os' module.
    """
    global _t
    if not _t:
        exclude_path = None
        if ignore_python_lib:
            exclude_path = os.path.realpath(os.path.dirname(os.__file__))

        init(exclude_path, None)
    
    _t.start()

def start_section(name):
    global _t
    _t.start_section(name)
    
def stop_section():
    global _t
    _t.stop_section()

def stop():
    """
    Stop tracing code coverage.
    """
    global _t
    if _t is not None:
        _t.stop()

def get_trace_obj():
    """
    Return the (singleton) trace object, if it exists.
    """
    return _t

def get_info(section_name=None):
    """
    Get the coverage dictionary from the trace object.
    """
    if _t:
        return get_data().gather_files(section_name)

#############

def display_ast():
    l = internals.LineGrabber(open(sys.argv[1]))
    l.pretty_print()
    print l.lines

def main():
    """
    Execute the given Python file with coverage, making it look like it is
    __main__.
    """
    ignore_pylibs = False

    # gather args

    n = 1
    figleaf_args = []
    for n in range(1, len(sys.argv)):
        arg = sys.argv[n]
        if arg.startswith('-'):
            figleaf_args.append(arg)
        else:
            break

    remaining_args = sys.argv[n:]

    usage = "usage: %prog [options] [python_script arg1 arg2 ...]"
    option_parser = OptionParser(usage=usage)

    option_parser.add_option('-i', '--ignore-pylibs', action="store_true",
                             dest="ignore_pylibs", default=False,
                             help="ignore Python library modules")

    (options, args) = option_parser.parse_args(args=figleaf_args)
    assert len(args) == 0

    if not remaining_args:
        option_parser.error("you must specify a python script to run!")

    ignore_pylibs = options.ignore_pylibs

    ## Reset system args so that the subsequently exec'd file can read
    ## from sys.argv
    
    sys.argv = remaining_args

    sys.path[0] = os.path.dirname(sys.argv[0])

    cwd = os.getcwd()

    start(ignore_pylibs)        # START code coverage

    import __main__
    try:
        execfile(sys.argv[0], __main__.__dict__)
    finally:
        stop()                          # STOP code coverage

        write_coverage(os.path.join(cwd, '.figleaf'))
