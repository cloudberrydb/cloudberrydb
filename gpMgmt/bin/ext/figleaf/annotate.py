"""
Common functions for annotating files with figleaf coverage information.
"""
import sys, os
from optparse import OptionParser
import ConfigParser
import re
import logging

import figleaf

thisdir = os.path.dirname(__file__)

try:                                    # 2.3 compatibility
    logging.basicConfig(format='%(message)s', level=logging.WARNING)
except TypeError:
    pass

logger = logging.getLogger('figleaf.annotate')

DEFAULT_CONFIGURE_FILE = ".figleafrc"

### utilities

def safe_conf_get(conf, section, name, default):
    try:
        val = conf.get(section, name)
    except (ConfigParser.NoSectionError, ConfigParser.NoOptionError):
        val = default

    return val

def configure(parser):
    """
    Configure the optparse.OptionParser object with defaults, optionally
    loaded from a configuration file.
    """
    CONFIG_FILE = os.environ.get('FIGLEAFRC', DEFAULT_CONFIGURE_FILE)
    
    parser.add_option("-c", "--coverage-file", action="store",
                       type="string", dest="coverage_file",
                       help="File containing figleaf coverage information.")
    
    parser.add_option("-s", "--sections-file", action="store",
                       type="string", dest="sections_file",
                       help="File containing figleaf sections coverage info.")

    parser.add_option("-v", "--verbose", action="store_true",
                      dest="verbose")

    conf_file = ConfigParser.ConfigParser()
    conf_file.read(CONFIG_FILE)         # ignores if not present

    default_coverage_file = safe_conf_get(conf_file,
                                          'figleaf', 'coverage_file',
                                          '.figleaf')
    default_sections_file = safe_conf_get(conf_file,
                                          'figleaf', 'sections_file',
                                          '.figleaf_sections')
    default_verbose = int(safe_conf_get(conf_file, 'figleaf', 'verbose',
                                        0))

    parser.set_defaults(coverage_file=default_coverage_file,
                        sections_file=default_sections_file,
                        verbose=default_verbose)

def filter_coverage(coverage, re_match):
    """
    ...
    """
    if not re_match:
        return coverage

    regexp = re.compile(re_match)
    
    d = {}
    for filename, lines in coverage.items():
        if regexp.match(filename):
            d[filename] = lines
            
    return d

### commands

def list(options, match=""):
    """
    List the filenames in the coverage file, optionally limiting it to
    those files matching to the regexp 'match'.
    """
    if options.verbose:
        print>>sys.stderr, '** Reading coverage from coverage file %s' % \
                           (options.coverage_file,)
        if match:
            print>>sys.stderr, '** Filtering against regexp "%s"' % (match,)
        
    coverage = figleaf.read_coverage(options.coverage_file)
    coverage = filter_coverage(coverage, match)

    for filename in coverage.keys():
        print filename

def list_sections(options, match=""):
    """
    List the filenames in the coverage file, optionally limiting it to
    those files matching to the regexp 'match'.
    """
    if options.verbose:
        print>>sys.stderr, '** Reading sections info from sections file %s' % \
                           (options.sections_file,)
        if match:
            print>>sys.stderr, '** Filtering against regexp "%s"' % (match,)

    fp = open(options.sections_file)
    figleaf.load_pickled_coverage(fp) # @CTB

    data = figleaf.internals.CoverageData(figleaf._t)
    coverage = data.gather_files()
    coverage = filter_coverage(coverage, match)

    for filename in coverage.keys():
        print filename

###

def read_exclude_patterns(filename):
    """
    Read in exclusion patterns from a file; these are just regexps.
    """
    if not filename:
        return []

    exclude_patterns = []

    fp = open(filename)
    for line in fp:
        line = line.rstrip()
        if line and not line.startswith('#'):
            pattern = re.compile(line)
        exclude_patterns.append(pattern)

    return exclude_patterns

def read_files_list(filename):
    """
    Read in a list of files from a file; these are relative or absolute paths.
    """
    s = {}
    for line in open(filename):
        f = line.strip()
        s[os.path.abspath(f)] = 1

    return s

def filter_files(filenames, exclude_patterns = [], files_list = {}):
    files_list = dict(files_list)       # make copy

    # list of files specified?
    if files_list:
        for filename in files_list.keys():
            yield filename

        filenames = [ os.path.abspath(x) for x in filenames ]
        for filename in filenames:
            try:
                del files_list[filename]
            except KeyError:
                logger.info('SKIPPING %s -- not in files list' % (filename,))
            
        return

    ### no files list given -- handle differently

    for filename in filenames:
        abspath = os.path.abspath(filename)
        
        # check to see if we match anything in the exclude_patterns list
        skip = False
        for pattern in exclude_patterns:
            if pattern.search(filename):
                logger.info('SKIPPING %s -- matches exclusion pattern' % \
                            (filename,))
                skip = True
                break

        if skip:
            continue

        # next, check to see if we're part of the figleaf package.
        if thisdir in filename:
            logger.debug('SKIPPING %s -- part of the figleaf package' % \
                         (filename,))
            continue

        # also, check for <string> (source file from things like 'exec'):
        if filename == '<string>':
            continue

        # miscellaneous other things: doctests
        if filename.startswith('<doctest '):
            continue

        yield filename

###

def main():
    parser = OptionParser()
    configure(parser)
    
    options, args = parser.parse_args()

    if not len(args):
        print "ERROR: You must specify a command like 'list' or 'report'.  Use"
        print "\n    %s -h\n" % (sys.argv[0],)
        print "for help on commands and options."
        sys.exit(-1)
        
    cmd = args.pop(0)

    if cmd == 'list':
        list(options, *args)
    elif cmd == 'list_sections':
        list_sections(options, *args)

    sys.exit(0)
