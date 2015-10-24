#!/usr/bin/env python
'''
gpfdist -- file distribution web server 

Usage: gpfdist [-?v] [-p port] [-d dir] [-q qchar] [-x xchar] [-h] [-l logfile]

         -?       : print this help screen
         -v       : verbose mode
         -p port  : which port to serve HTTP. Default to 8080
         -d dir   : serve files under the specified directory. 
                    Default to '.'
         -q qchar : set quote character. If not specified, the
                    program will not parse for CSV, and will 
                    assume each record is separated by a newline
                    character that never occurs inside a record.
         -x xchar : set escape character (default to qchar)
         -h       : first data row in data is a header row. skip 
                    it (only allowed in CSV format).
		 -l       : fully qualified path and name of log file 
	
'''

import SocketServer, BaseHTTPServer, os, sys, getopt, threading, time, socket

MAX_CONCURRENT_SESSION = 64
opt = {}
opt['-p'] = 8080
opt['-v'] = False
opt['-V'] = False
opt['-h'] = False
opt['-d'] = '.'
qc = None
xc = None

def usage(exitarg):
    print __doc__
    sys.exit(exitarg)

def parseInt(val):
    try: return int(val)
    except ValueError: return 0

def parseCommandLine():
    global opt, qc, xc
    try:
	(options, args) = getopt.getopt(sys.argv[1:], '?Vvhp:d:q:x:')
    except Exception, e:
	usage('Error: ' + str(e))

    for (switch, val) in options:
	if (switch == '-?'):		usage(0)
        elif (switch[1] in 'Vvh'):	opt[switch] = True
        elif (switch[1] in 'dqx'):      opt[switch] = val
        elif (switch[1] in 'p'):	opt[switch] = parseInt(val)
        elif (switch == '-q'):          qc = val; opt['-f'] = False
        elif (switch == '-x'):          xc = val; opt['-f'] = False

    if not opt['-p'] > 0: 
	usage('Error: please specify port number for -p switch')

    if not os.path.isdir(opt['-d']):
	usage('Error: please specify a directory for -d switch')

    opt['-d'] = os.path.abspath(opt['-d'])
    if (opt['-d'] == '/'):
	usage('Security Error: cannot run under root (/) directory')
    
    if '-q' in opt: qc = opt['-q']; opt['-f'] = False
    if '-x' in opt: xc = opt['-x']; opt['-f'] = False

    if qc and len(qc) != 1: 
	usage('Error: please specify a character for -q switch')

    if xc and len(xc) != 1:
	usage('Error: please specify a character for -x switch')
    
    if not qc and xc: 
	usage('Error: you must specify -q qchar with -x xchar')

    if not qc and opt['-h']:
	usage('Error: header may only be used in CSV format. please specify -q switch')
    
    if len(args) != 0:
	usage(1)

    if opt['-V']: opt['-v'] = true


# a File Session - shared among all GET request threads
class Session:
    def __init__(self, fd, fname):
	self.m_fname = fname
	self.m_fd = fd
	self.m_sem = threading.Semaphore(1)
	self.m_residue = None
	self.m_off = 0
	self.m_max = 0
	self.m_linecnt = 0
	self.m_threadcnt = 0

    def readLine(self):
	line = ''
	inQuote = False
	lastWasEsc = False
	self.m_sem.acquire()
	start = self.m_off
	try:
	    while True:
		if not self.m_residue:
		    if not self.m_fd: break
		    self.m_residue = self.m_fd.read(1024*64)
		    if not self.m_residue:
			self.m_fd.close()
			self.m_fd = None
			break
		    start = 0
		    self.m_off = 0
		    self.m_max = len(self.m_residue)
		elif (self.m_off >= self.m_max):
		    line = line + self.m_residue[start:]
		    self.m_residue = None
		    continue

		c = self.m_residue[self.m_off]
		self.m_off = self.m_off + 1
		
		if c == '\n' and not inQuote: 
		    line = line + self.m_residue[start:self.m_off]
		    break

		if inQuote and c == xc:
		    lastWasEsc = not lastWasEsc
		if c == qc and not lastWasEsc:
		    inQuote = not inQuote
		if c != xc:
		    lastWasEsc = False

	finally:
	    self.m_sem.release()
	
	self.m_linecnt = self.m_linecnt + 1
	if self.m_linecnt % 10000 == 0: 
	    print self.m_linecnt, 'lines'

	return line

    def readFile(self):
	lines = []
	self.m_sem.acquire()
	try:
	    if self.m_fd:
		if self.m_residue:
		    lines.append(self.m_residue);
		    self.m_residue = None
		while not self.m_residue and self.m_fd:
		    chunk = self.m_fd.read(1024*64)
		    if not chunk:
			self.m_fd.close()
			self.m_fd = None
		    else:
			c = chunk.split('\n', 1)
			if (len(c) == 1):
			    lines.append(c[0])
			else:
			    lines.append(c[0])
			    lines.append('\n')
			    self.m_residue = c[1]
	finally:
	    self.m_sem.release()

	return lines

#
#  Session dictionary 
#    sess[TID][fname] is a Session object 
#
sess = {}
sessSem = threading.Semaphore(1)
def findSession(TID, fname):
    global sess, sessSem
    key = (TID, fname)
    sessSem.acquire()
    try:
	if key not in sess:
	    fd = open(fname, 'r', 1024*1024)
	    sess[key] = Session(fd, fname)
	    if opt['-v']: print '[INFO] initiated session', key
	else:
	    if opt['-v']: print '[INFO] joined session', key
    finally:
	sessSem.release();

    return sess[key]


#
#  Class to handle individual request
#
class GPFDistRequestHandler(BaseHTTPServer.BaseHTTPRequestHandler):

    counter = 0

    def log_request(self, code):
	print "%s - %s" % (self.client_address[0], code)


    def send400(self, msg):
	print 'ERROR: %s' % msg
	self.send_response(400)
	return None

    def send200empty(self):
	if opt['-v']: print '  [ignore] thread %s' % self.client_address[0]
	self.send_response(200)
	self.send_header("Content-type", "text/plain")
	self.send_header("Content-length", "0")
	self.end_headers()
	return None

    def do_GET(self):
	try:
	    GPFDistRequestHandler.counter = GPFDistRequestHandler.counter + 1
	    TID = self.headers.getheader('X-GP-TID')
	    if not TID:
		# start an non-transaction session
		TID = 'auto-tid.' + str(GPFDistRequestHandler.counter)

	    fname = self.path
	    if fname.find('/') == 0: fname = fname[1:]
	    fname = os.path.join(opt['-d'], fname)
	    fname = os.path.normpath(fname)
	    if fname.find(opt['-d']) != 0:
		msg = 'bad path specified (%s)' % (self.path)
		return self.send400(msg)

	    try:
		s = findSession(TID, fname)
		if not s:
		    msg = 'unable to serve TID %s' % TID
		    return self.send400(msg)
		if s.m_threadcnt >= MAX_CONCURRENT_SESSION:
		    return send200empty(self)
		s.m_threadcnt = s.m_threadcnt + 1
	    except IOError, e:
		msg = str(e)
		return self.send400(msg)

	    self.send_response(200)
	    self.send_header("Content-type", "text/plain")
	    self.end_headers()

	    # parse in CSV format if quote char was specified
	    if qc:
		# skip the first line if 'header' option was specified
		if opt['-h']:
			line = s.readLine()
		while 1:
		    line = s.readLine()
		    if not line: break
		    self.wfile.write(line)
	    # parse in text format otherwise
	    else:
		while 1:
		    chunks = s.readFile()
		    if not chunks: break
		    for c in chunks:
			self.wfile.write(c)

	    s.m_threadcnt = s.m_threadcnt - 1
	    if opt['-V']: print '  %s done' % fname
	    if TID[0] == 'a' and TID.find('auto-tid.') == 0:
		del sess[(TID, fname)]

	except socket.error, e:
	    print 'socket error: ', str(e), 'while serving', self.path


class GPFDistServer(SocketServer.ThreadingTCPServer):
    
    allow_reuse_address = 1
    request_queue_size = 256

    def server_bind(self):
	SocketServer.ThreadingTCPServer.server_bind(self)
	host, port = self.socket.getsockname()[:2]
	self.server_name = socket.getfqdn(host)
	self.server_post = port


try:
    parseCommandLine()
    serverAddress = ('', opt['-p'])
    GPFDistRequestHandler.protocol_version = "HTTP/1.0"
    httpd = GPFDistServer(serverAddress, GPFDistRequestHandler)
    sa = httpd.socket.getsockname()
    print "Serving HTTP on %s:%d, directory %s ..." % (sa[0], sa[1], os.path.abspath(opt['-d']))
    httpd.serve_forever()

except KeyboardInterrupt:
    sys.exit('[Interrupted ...]')

