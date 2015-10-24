from pexpect import *
#import os, sys, getopt, shutil

class pxssh (spawn):
    """This class extends pexpect.spawn to specialize setting up SSH connections.
    This adds methods for login, logout, and expecting the prompt.
    It does various hacky things to handle any situation in the SSH login process.
    For example, if the session is your first login, then it automatically
    accepts the certificate; or if you have public key authentication setup
    and you don't need a password then this is OK too.

    Example usage that runs 'ls -l' on server and prints the result:
        import pxssh
        s = pxssh.pxssh()
        if not s.login ('localhost', 'myusername', 'mypassword'):
            print "SSH session failed on login."
            print str(s)
        else:
            print "SSH session login successful"
            s.sendline ('ls -l')
            s.prompt()           # match the prompt
            print s.before     # print everything before the prompt.
            s.logout()
    """

    def __init__ (self):
        # SUBTLE HACK ALERT!
        # Note that the command to set the prompt uses a slightly different string
        # than the regular expression to match it. This is because when you set the
        # prompt the command will echo back, but we don't want to match the echoed
        # command. So if we make the set command slightly different than the regex
        # we eliminate the problem. To make the set command different we add a
        # backslash in front of $. The $ doesn't need to be escaped, but it doesn't
        # hurt and serves to make the set command different than the regex.
        self.PROMPT = "\[PEXPECT\][\$\#] " # used to match the command-line prompt.
        # used to set shell command-line prompt to something more unique.
        self.PROMPT_SET_SH = "PS1='[PEXPECT]\$ '"
        self.PROMPT_SET_CSH = "set prompt='[PEXPECT]\$ '"
        self.SET_TERM_DUMB_SH = "TERM=dumb"
        self.SET_TERM_DUMB_CSH = "set TERM=dumb"

    ### cktan: spawn login process, but don't wait on it
    def loginAsync (self,server,username=None,login_timeout=10, port=None):
	cmd = 'ssh -o "BatchMode yes" -o "StrictHostKeyChecking no"'
        if port:
            cmd = cmd + ' -p %d' % port
        if username:
            cmd = cmd + ' -l %s' % username
        cmd = cmd + ' ' + server
        spawn.__init__(self, cmd, timeout=login_timeout)
	# we don't need this since we are not sending 
	# password over (see comments in pexpect.py re: delaybeforesend)
	self.delaybeforesend = 0

    ### cktan: wait for login
    def loginWait(self, login_timeout=10, set_term_dumb=False):
        #, "(?i)no route to host"])
	echo = 'hello hello hello hello'
	self.sendline('echo ' + echo)
	exp = [echo, "(?i)permission denied", "(?i)terminal type", 
	       TIMEOUT, "(?i)connection closed by remote host", 
	       EOF]
	try:
	    i = self.expect(exp)
	    if i == 0:
		i = self.expect(exp)

	    if i == 0: 
		pass
	    elif i == 1: #permission denied
		self.close()
		return False
	    elif i == 2: #terminal type not handled
		self.close()
		return False
	    elif i == 3: #timeout
		# This is tricky... presume that we are at the command-line prompt.
		# It may be that the prompt was so weird that we couldn't match it.
		pass
	    elif i == 4: #connection closed by remote host
		self.close()
		return False
	    elif i == 5: #EOF
		self.close()
		return False
	    else:  #unexpected
		self.close()
		return False

	    self.sendline('exec bash')
	    if not self._set_unique_prompt():
		self.close()
		return False

	    if set_term_dumb: self._set_term_dumb()

	    return True
	except EOF:
	    self.close()
	    return False


    ### cktan: added <port> parameter
    def login (self,server,username,login_timeout=10, port=22):
	self.loginAsync(server, username, login_timeout, port)
	return self.loginWait(login_timeout)

    def logout (self):
        """This sends exit. If there are stopped jobs then this sends exit twice.
        """
        self.sendline("exit")
        index = self.expect([EOF, "(?i)there are stopped jobs"])
        if index==1:
            self.sendline("exit")
            self.expect(EOF)

    def prompt (self, timeout=20):
        """This expects the prompt. This returns True if the prompt was matched.
        This returns False if there was a timeout.
        """
        i = self.expect([self.PROMPT, TIMEOUT], timeout=timeout)
        if i==1:
            return False
        return True
        
    def _set_unique_prompt (self, optional_prompt=None):
        """This attempts to reset the shell prompt to something more unique.
            This makes it easier to match unambiguously.
        """
        if optional_prompt is not None:
            self.prompt = optional_prompt
        self.sendline (self.PROMPT_SET_SH) # sh-style
        i = self.expect ([TIMEOUT, self.PROMPT], timeout=10)
        if i == 0: # csh-style
            self.sendline (self.PROMPT_SET_CSH)
            i = self.expect ([TIMEOUT, self.PROMPT], timeout=10)
            if i == 0:
                return 0
        return 1

    def _set_term_dumb(self):
        self.sendline(self.SET_TERM_DUMB_SH)
        i = self.expect([TIMEOUT, self.PROMPT], timeout=10)
        if i == 0: # csh-style
            self.sendline(self.SET_TERM_DUMB_CSH)
            i = self.expect([TIMEOUT, self.PROMPT], timeout=10)
            if i == 0:
                return 0
        return 1
