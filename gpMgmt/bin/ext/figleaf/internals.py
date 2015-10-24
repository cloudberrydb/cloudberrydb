"""
Coverage tracking internals.
"""

import sys
import threading

err = sys.stderr

import types, symbol

# use builtin sets if in >= 2.4, otherwise use 'sets' module.
try:
    set()
except NameError:
    from sets import Set as set

def get_interesting_lines(code):
    """
    Count 'interesting' lines of Python in a code object, where
    'interesting' is defined as 'lines that could possibly be
    executed'.

    This is done by dissassembling the code objecte and returning
    line numbers.
    """

    # clean up weird end-of-file issues

    lines = set([ l for (o, l) in findlinestarts(code) ])
    for const in code.co_consts:
        if type(const) == types.CodeType:
            lines.update(get_interesting_lines(const))

    return lines

def findlinestarts(code):
    """Find the offsets in a byte code which are start of lines in the source.

    Generate pairs (offset, lineno) as described in Python/compile.c.

    CTB -- swiped from Python 2.5, module 'dis', so that earlier versions
    of Python could use the function, too.
    """
    byte_increments = [ord(c) for c in code.co_lnotab[0::2]]
    line_increments = [ord(c) for c in code.co_lnotab[1::2]]

    lastlineno = None
    lineno = code.co_firstlineno
    addr = 0
    for byte_incr, line_incr in zip(byte_increments, line_increments):
        if byte_incr:
            if lineno != lastlineno:
                yield (addr, lineno)
                lastlineno = lineno
            addr += byte_incr
        lineno += line_incr
    if lineno != lastlineno:
        yield (addr, lineno)

class CodeTracer:
    """
    Basic mechanisms for code coverage tracking, using sys.settrace.  
    """
    def __init__(self, exclude_prefix, include_only_prefix):
        self.common = self.c = set()
        self.section_name = None
        self.sections = {}
        
        self.started = False

        assert not (exclude_prefix and include_only_prefix), \
               "mutually exclusive"
        
        self.excl = exclude_prefix
        self.incl = include_only_prefix

    def start(self):
        """
        Start recording.
        """
        if not self.started:
            self.started = True

            if self.excl and not self.incl:
                global_trace_fn = self.g1
            elif self.incl and not self.excl:
                global_trace_fn = self.g2
            else:
                global_trace_fn = self.g0

            sys.settrace(global_trace_fn)

            if hasattr(threading, 'settrace'):
                threading.settrace(global_trace_fn)

    def stop(self):
        if self.started:
            sys.settrace(None)
            
            if hasattr(threading, 'settrace'):
                threading.settrace(None)

            self.started = False
            self.stop_section()

    def g0(self, f, e, a):
        """
        global trace function, no exclude/include info.

        f == frame, e == event, a == arg        .
        """
        if e == 'call':
            return self.t

    def g1(self, f, e, a):
        """
        global trace function like g0, but ignores files starting with
        'self.excl'.
        """
        if e == 'call':
            excl = self.excl
            path = f.f_globals.get('__file__')
            if path is None:
                path = f.f_code.co_filename

            if excl and path.startswith(excl):
                return

            return self.t

    def g2(self, f, e, a):
        """
        global trace function like g0, but only records files starting with
        'self.incl'.
        """
        if e == 'call':
            incl = self.incl
            if incl and f.f_code.co_filename.startswith(incl):
                return self.t

    def t(self, f, e, a):
        """
        local trace function.
        """
        if e is 'line':
            self.c.add((f.f_code.co_filename, f.f_lineno))
        return self.t

    def clear(self):
        """
        wipe out coverage info
        """

        self.c = {}

    def start_section(self, name):
        self.stop_section()

        self.section_name = name
        self.c = self.sections.get(name, set())
        
    def stop_section(self):
        if self.section_name:
            self.sections[self.section_name] = self.c
            self.section_name = None
            self.c = self.common

class CoverageData:
    """
    A class to manipulate and combine data from the CodeTracer object.

    In general, do not pickle this object; it's simpler and more
    straightforward to just pass the basic Python objects around
    (e.g. CoverageData.common, a set, and CoverageData.sections, a
    dictionary of sets).
    """
    def __init__(self, trace_obj=None):
        self.common = set()
        self.sections = {}
        
        if trace_obj:
            self.update(trace_obj)
            
    def update(self, trace_obj):
        # transfer common-block code coverage -- if no sections are set,
        # this will be all of the code coverage info.
        self.common.update(trace_obj.common)

        # update our internal section dictionary with the (filename, line_no)
        # pairs from the section coverage as well.
        
        for section_name, section_d in trace_obj.sections.items():
            section_set = self.sections.get(section_name, set())
            section_set.update(section_d)
            self.sections[section_name] = section_set

    def gather_files(self, name=None):
        """
        Return the dictionary of lines of executed code; the dict
        keys are filenames and values are sets containing individual
        (integer) line numbers.
        
        'name', if set, is the desired section name from which to gather
        coverage info.
        """
        cov = set()
        cov.update(self.common)

        if name is None:
            for section_name, coverage_set in self.sections.items():
                cov.update(coverage_set)
        else:
            coverage_set = self.sections.get(name, set())
            cov.update(coverage_set)
            
#        cov = list(cov)
#        cov.sort()

        files = {}
        for (filename, line) in cov:    # @CTB could optimize
            d = files.get(filename, set())
            d.add(line)
            files[filename] = d

        return files

    def gather_sections(self, file):
        """
        Return a dictionary of sets containing section coverage information for
        a specific file.  Dict keys are sections, and the dict values are
        sets containing (integer) line numbers.
        """
        sections = {}
        for k, c in self.sections.items():
            s = set()
            for (filename, line) in c.keys():
                if filename == file:
                    s.add(line)
            sections[k] = s
        return sections
