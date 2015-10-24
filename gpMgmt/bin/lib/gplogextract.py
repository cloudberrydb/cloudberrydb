#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
  gplogextract.py [options] file ...

General Options:
  -? | --help                show this help message and exit
  -v | --version             show the program's version number and exit

Options:
  -o | --output DIRECTORY    output directory
'''
# ============================================================================
__version__ = '$Revision: #1 $'

# ============================================================================
import sys, os
EXECNAME = os.path.split(__file__)[-1]

# ============================================================================
# Python version 2.6.2 is expected, must be between 2.5-3.0
# ============================================================================
if sys.version_info < (2, 5, 0) or sys.version_info >= (3, 0, 0):
    sys.stderr.write("""
Error: %s is supported on Python versions 2.5 or greater
Please upgrade python installed on this machine.
""" % EXECNAME)
    sys.exit(1)

# ============================================================================
import traceback                         # exception logging
import optparse                          # Option Parsing
import csv                               # CSV format file parsing

# ============================================================================
# Some basic exceptions:
# ============================================================================
class GPError(StandardError): pass

# ============================================================================
def cli_help():
    '''
    Reads the help file from the docs directory, if documentation file can't
    be found this defaults to the __doc__ string.
    '''
    help_path = os.path.join(sys.path[0], '../docs/cli_help', 
                             EXECNAME + '_help')
    f = None
    try:
        try:
            f = open(help_path)
            return f.read(-1)
        except:
            return __doc__
    finally:
        if f: 
            f.close()

def usage():
    print cli_help()

# ============================================================================
def ParseOptions():
    '''
    Parses command line options input to the script.
    '''
    # output = os.getcwd()
    output = None

    # Setup Options Parser
    parser = optparse.OptionParser(usage=cli_help(), add_help_option=False)
    parser.add_option('-v', '--version', default=False, action='store_true')
    parser.add_option('-?', '--help', default=False, action='store_true')
    parser.add_option('-o', '--output', default=output)
    
    (options, args) = parser.parse_args()

    # Print version and exit
    if options.version:
        print EXECNAME + ' ' + __version__
        sys.exit(0)

    # Print help and exit
    if options.help:
        usage()
        sys.exit(0)

    if len(args) < 1:
        usage()
        sys.exit(1)

    return (options, args)


# ============================================================================
# main()
# ============================================================================
if __name__ == '__main__':
    (opt, args) = ParseOptions()

    try:
        if opt.output:
            filename = os.path.join(opt.output, 'extracted.sql')
            filename = os.path.abspath(filename)
            output = open(filename, 'w')
        else:
            output = sys.stdout

        for input in args:
            f = open(input)
            reader = csv.reader(f, delimiter=',', quotechar='"')
            for row in reader:
                if len(row) != 30:
                    raise GPError("Unable to parse file: %s" % input)
                time     = row[0]
                user     = row[1]
                database = row[2]
                type     = row[5]
                session  = row[9]
                cmd      = row[10]
                kind     = row[16]
                payload  = row[18]
                if payload.lower().find('statement:') >= 0:
                    if payload[-1] != ';': 
                        payload += ';'
                    output.writelines([
                            "-- User:      %s\n" % user,
                            "-- Database:  %s\n" % database,
                            "-- Session:   %s\n" % session,
                            "-- Timestamp: %s\n" % time,
                            "%s\n\n" % payload[11:],
                            ])
                
                if kind in ('ERROR', 'FATAL'):
                    payload.replace("\n", "\n-- ")
                    output.writelines(["-- %s: %s\n" % (kind, payload)])


    # An expected error
    except GPError, e:
        logger.error(str(e))
        logger.info("exit(1)")
        sys.exit(1)

    # User cancelation 
    except KeyboardInterrupt, e:
        print '[Cancelled]'
        sys.exit(1)

    # Let SystemExit exceptions through
    except SystemExit, e:
        raise e

    # Catch anything else - shouldn't ever occur
    except BaseException, e:
        sys.stderr.write('[ERROR] Unhandled error occurred\n')
        sys.stderr.write(traceback.format_exc())
        sys.exit(1)

