import figleaf
import os
import re

# use builtin sets if in >= 2.4, otherwise use 'sets' module.
try:
    set()
except NameError:
    from sets import Set as set

from figleaf.annotate import read_exclude_patterns, filter_files, logger, \
     read_files_list

###

def annotate_file(fp, lines, covered):
    # initialize
    n_covered = n_lines = 0

    output = []
    for i, line in enumerate(fp):
        is_covered = False
        is_line = False

        i += 1

        if i in covered:
            is_covered = True

            n_covered += 1
            n_lines += 1
        elif i in lines:
            is_line = True

            n_lines += 1

        color = 'black'
        if is_covered:
            color = 'green'
        elif is_line:
            color = 'red'

        line = escape_html(line.rstrip())
        output.append('<font color="%s">%4d. %s</font>' % (color, i, line))

    try:
        percent = n_covered * 100. / n_lines
    except ZeroDivisionError:
        percent = 100

    return output, n_covered, n_lines, percent

def write_html_summary(info_dict, directory):
    info_dict_items = info_dict.items()

    def sort_by_percent(a, b):
        a = a[1][2]
        b = b[1][2]

        return -cmp(a,b)
    info_dict_items.sort(sort_by_percent)

    summary_lines = sum([ v[0] for (k, v) in info_dict_items])
    summary_cover = sum([ v[1] for (k, v) in info_dict_items])

    summary_percent = 100
    if summary_lines:
        summary_percent = float(summary_cover) * 100. / float(summary_lines)


    percents = [ float(v[1]) * 100. / float(v[0])
                 for (k, v) in info_dict_items if v[0] ]
    
    percent_90 = [ x for x in percents if x >= 90 ]
    percent_75 = [ x for x in percents if x >= 75 ]
    percent_50 = [ x for x in percents if x >= 50 ]

    ### write out summary.

    index_fp = open('%s/index.html' % (directory,), 'w')
    index_fp.write('''
<html>
<title>figleaf code coverage report</title>
<h2>Summary</h2>
%d files total: %d files &gt; 90%%, %d files &gt; 75%%, %d files &gt; 50%%
<p>
<table border=1>
<tr>
 <th>Filename</th><th># lines</th><th># covered</th><th>%% covered</th>
</tr>

<tr>
 <td><b>totals:</b></td>
 <td><b>%d</b></td>
 <td><b>%d</b></td>
 <td><b>%.1f%%</b></td>
</tr>

<tr></tr>

''' % (len(percents), len(percent_90), len(percent_75), len(percent_50),
       summary_lines, summary_cover, summary_percent,))

    for filename, (n_lines, n_covered, percent_covered,) in info_dict_items:
        html_outfile = make_html_filename(filename)

        index_fp.write('''
<tr>
 <td><a href="./%s">%s</a></td>
 <td>%d</td>
 <td>%d</td>
 <td>%.1f</td>
</tr>
''' % (html_outfile, filename, n_lines, n_covered, percent_covered,))

    index_fp.write('</table>\n')
    index_fp.close()
    

def report_as_html(coverage, directory, exclude_patterns, files_list):
    """
    Write an HTML report on all of the files, plus a summary.
    """

    ### now, output.

    keys = coverage.keys()
    info_dict = {}
    for pyfile in filter_files(keys, exclude_patterns, files_list):

        try:
            fp = open(pyfile, 'rU')
            lines = figleaf.get_lines(fp)
        except KeyboardInterrupt:
            raise
        except IOError:
            logger.error('CANNOT OPEN: %s' % (pyfile,))
            continue
        except Exception, e:
            logger.error('ERROR: file %s, exception %s' % (pyfile, str(e)))
            continue

        #
        # ok, we want to annotate this file.  now annotate file ==> html.
        #

        # initialize
        covered = coverage.get(pyfile, set())

        # rewind
        fp.seek(0)

        # annotate
        output, n_covered, n_lines, percent = annotate_file(fp, lines, covered)

        # summarize
        info_dict[pyfile] = (n_lines, n_covered, percent)

        # write out the file
        html_outfile = make_html_filename(pyfile)
        html_outfile = os.path.join(directory, html_outfile)
        html_outfp = open(html_outfile, 'w')
        
        html_outfp.write('source file: <b>%s</b><br>\n' % (pyfile,))
        html_outfp.write('''

file stats: <b>%d lines, %d executed: %.1f%% covered</b>
<pre>
%s
</pre>

''' % (n_lines, n_covered, percent, "\n".join(output)))
            
        html_outfp.close()

        logger.info('reported on %s' % (pyfile,))

    ### print a summary, too.
    write_html_summary(info_dict, directory)

    logger.info('reported on %d file(s) total\n' % len(info_dict))

def prepare_reportdir(dirname):
    "Create output directory."
    try:
        os.mkdir(dirname)
    except OSError:                         # already exists
        pass

def make_html_filename(orig):
    "'escape' original paths into a single filename"

    orig = os.path.abspath(orig)
#    orig = os.path.basename(orig)
    orig = os.path.splitdrive(orig)[1]
    orig = orig.replace('_', '__')
    orig = orig.replace(os.path.sep, '_')
    orig += '.html'
    return orig

def escape_html(s):
    s = s.replace("&", "&amp;")
    s = s.replace("<", "&lt;")
    s = s.replace(">", "&gt;")
    s = s.replace('"', "&quot;")
    return s

def main():
    import sys
    import logging
    from optparse import OptionParser
    ###

    usage = "usage: %prog [options] [coverage files ... ]"
    option_parser = OptionParser(usage=usage)

    option_parser.add_option('-x', '--exclude-patterns', action="store",
                             dest="exclude_patterns_file",
        help="file containing regexp patterns of files to exclude from report")

    option_parser.add_option('-f', '--files-list', action="store",
                             dest="files_list",
                             help="file containing filenames to report on")

    option_parser.add_option('-d', '--output-directory', action='store',
                             dest="output_dir",
                             default = "html",
                             help="directory for HTML output")

    option_parser.add_option('-q', '--quiet', action='store_true',
                             dest='quiet',
                             help='Suppress all but error messages')
    
    option_parser.add_option('-D', '--debug', action='store_true',
                             dest='debug',
                             help='Show all debugging messages')

    (options, args) = option_parser.parse_args()

    if options.quiet:
        logging.disable(logging.DEBUG)
        
    if options.debug:
        logger.setLevel(logging.DEBUG)

    ### load/combine

    if not args:
        args = ['.figleaf']

    coverage = {}
    for filename in args:
        logger.debug("loading coverage info from '%s'\n" % (filename,))
        try:
            d = figleaf.read_coverage(filename)
            coverage = figleaf.combine_coverage(coverage, d)
        except IOError:
            logger.error("cannot open filename '%s'\n" % (filename,))

    if not coverage:
        logger.warning('EXITING -- no coverage info!\n')
        sys.exit(-1)

    exclude = []
    if options.exclude_patterns_file:
        exclude = read_exclude_patterns(options.exclude_patterns_file)

    files_list = {}
    if options.files_list:
        files_list = read_files_list(options.files_list)

    ### make directory
    prepare_reportdir(options.output_dir)
    report_as_html(coverage, options.output_dir, exclude, files_list)

    print 'figleaf: HTML output written to %s' % (options.output_dir,)
