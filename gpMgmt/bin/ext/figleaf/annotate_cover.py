import figleaf
import os
import re

from annotate import read_exclude_patterns, filter_files, logger

def report_as_cover(coverage, exclude_patterns=[], ):
    ### now, output.

    keys = coverage.keys()
    info_dict = {}
    
    for k in filter_files(keys):
        try:
            pyfile = open(k, 'rU')
            lines = figleaf.get_lines(pyfile)
        except IOError:
            logger.warning('CANNOT OPEN: %s' % k)
            continue
        except KeyboardInterrupt:
            raise
        except Exception, e:
            logger.error('ERROR: file %s, exception %s' % (pyfile, str(e)))
            continue

        # ok, got all the info.  now annotate file ==> html.

        covered = coverage[k]
        pyfile = open(k)
        (n_covered, n_lines, output) = make_cover_lines(lines, covered, pyfile)


        try:
            pcnt = n_covered * 100. / n_lines
        except ZeroDivisionError:
            pcnt = 100
        info_dict[k] = (n_lines, n_covered, pcnt)

        outfile = make_cover_filename(k)
        try:
            outfp = open(outfile, 'w')
            outfp.write("\n".join(output))
            outfp.write("\n")
            outfp.close()
        except IOError:
            logger.warning('cannot open filename %s' % (outfile,))
            continue

        logger.info('reported on %s' % (outfile,))

    ### print a summary, too.

    info_dict_items = info_dict.items()

    def sort_by_pcnt(a, b):
        a = a[1][2]
        b = b[1][2]

        return -cmp(a,b)

    info_dict_items.sort(sort_by_pcnt)

    logger.info('reported on %d file(s) total\n' % len(info_dict))
    return len(info_dict)

def make_cover_lines(line_info, coverage_info, fp):
    n_covered = n_lines = 0
    output = []
    
    for i, line in enumerate(fp):
        is_covered = False
        is_line = False

        i += 1

        if i in coverage_info:
            is_covered = True
            prefix = '+'

            n_covered += 1
            n_lines += 1
        elif i in line_info:
            prefix = '-'
            is_line = True

            n_lines += 1
        else:
            prefix = '0'

        line = line.rstrip()
        output.append(prefix + ' ' + line)
    
    return (n_covered, n_lines, output)

def make_cover_filename(orig):
    return orig + '.cover'

def main():
    import sys
    import logging
    from optparse import OptionParser
    
    ###

    option_parser = OptionParser()

    option_parser.add_option('-x', '--exclude-patterns', action="store",
                             dest="exclude_patterns_file",
                             help="file containing regexp patterns to exclude")

    option_parser.add_option('-q', '--quiet', action='store_true',
                             dest='quiet',
         help="file containig regexp patterns of files to exclude from report")
    
    option_parser.add_option('-D', '--debug', action='store_true',
                             dest='debug',
                             help='Show all debugging messages')

    (options, args) = option_parser.parse_args()

    if options.quiet:
        logging.disable(logging.DEBUG)

    if options.debug:
        logger.setLevel(logging.DEBUG)

    ### load

    if not args:
        args = ['.figleaf']

    coverage = {}
    for filename in args:
        logger.debug("loading coverage info from '%s'\n" % (filename,))
        d = figleaf.read_coverage(filename)
        coverage = figleaf.combine_coverage(coverage, d)

    if not coverage:
        logger.warning('EXITING -- no coverage info!\n')
        sys.exit(-1)

    exclude = read_exclude_patterns(options.exclude_patterns_file)
    report_as_cover(coverage, exclude)
