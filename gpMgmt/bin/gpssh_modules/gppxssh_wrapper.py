from datetime import datetime
import time
import sys

sys.path.insert(1, sys.path[0] + '/lib')
from pexpect import pxssh, TIMEOUT

CRNL = '\r\n'
DEBUG_VERBOSE_PRINTING = False
# experimentally derived that a sequence of tries with delays of
#  1, 5, 25, 125 secs worked to surmount a 1-second delay (via `tc` test)
RETRY_EXPONENT = 5


class PxsshWrapper(pxssh.pxssh):
    def __init__(self, delaybeforesend, sync_retries, options):
        self.sync_retries = sync_retries
        super(PxsshWrapper, self).__init__(delaybeforesend=delaybeforesend, options=options)

    def sync_original_prompt(self, sync_multiplier=1.0):
        """
        override the pxssh method to allow retries with extended timeout intervals
        """
        # make two attempts to throw away, perhaps emptying any initial Message Of The Day.
        # In practice, the first request can take a huge amount of time, compared to subsequent requests,
        # so give it a 5-second max wait as a special case.
        self.sendline()
        self.wait_for_any_response(max_wait_secs=5)
        self.clear_response_channel()

        self.sendline()
        self.try_read_prompt(sync_multiplier)

        # Each retry should go more slowly to accommodate possible cpu load, network,
        # or other issues on the segment host that might delay when we receive the prompt.
        num_retries = self.sync_retries
        retry_attempt = 0
        success = False
        while (not success) and retry_attempt <= num_retries:
            # each retry will get an exponentially longer timeout interval
            sync_multiplier_for_this_retry = sync_multiplier * (RETRY_EXPONENT ** retry_attempt)
            start = time.time()

            if DEBUG_VERBOSE_PRINTING:
                sys.stderr.write("\nUsing sync multiplier: %f\n" % sync_multiplier_for_this_retry)

            self.sendline()
            if DEBUG_VERBOSE_PRINTING:
                sys.stderr.write("\nstart try read: %s\n" % datetime.now())
            first_prompt = self.try_read_prompt(sync_multiplier_for_this_retry)
            if DEBUG_VERBOSE_PRINTING:
                sys.stderr.write("\nend try read: %s\n" % datetime.now())
            self.sendline()
            second_prompt = self.try_read_prompt(sync_multiplier_for_this_retry)

            success = self.are_prompts_similar(first_prompt, second_prompt)
            if not success:
                retry_attempt += 1
                if retry_attempt <= num_retries:
                    # This attempt has failed to allow enough time.
                    # We want to "clear the runway" before another attempt.
                    # The next attempt will have a wait that is exponent times as long.
                    # To clear the runway, we sleep for about as long as this upcoming retry.
                    # Thus, the overall duration of this retry cycle becomes
                    # roughly equivalent to the timeout used by the next attempt
                    time.sleep(RETRY_EXPONENT * sync_multiplier_for_this_retry)
                    self.clear_response_channel()

        if DEBUG_VERBOSE_PRINTING:
            if not success:
                sys.stderr.write('\nAfter %d retries, prompts failed to be consistent.\n' % num_retries)
            elif retry_attempt > 0:
                sys.stderr.write('\nConsistent prompts after extending timeout, with %i retries.\n' % retry_attempt)
            sys.stderr.flush()

        return success

    def clear_response_channel(self):
        """remove any readily-available characters. stop as soon as even a little wait time is discovered"""
        if DEBUG_VERBOSE_PRINTING:
            sys.stderr.write('\nflushing:\n')
        prompt = "dummy non empty"
        while prompt:
            try:
                prompt = self.read_nonblocking(size=1, timeout=0.01)
                if DEBUG_VERBOSE_PRINTING:
                    sys.stderr.write(prompt)
            except TIMEOUT:
                break
        if DEBUG_VERBOSE_PRINTING:
            sys.stderr.write('\n')

    def wait_for_any_response(self, max_wait_secs=5):
        if DEBUG_VERBOSE_PRINTING:
            sys.stderr.write('\nstart looking for a character at %s\n' % datetime.now())
        duration = 0
        while duration < max_wait_secs:
            start = time.time()
            try:
                prompt = self.read_nonblocking(size=1, timeout=0.01)
                if prompt:
                    break
            except TIMEOUT:
                duration += time.time() - start
        if DEBUG_VERBOSE_PRINTING:
            sys.stderr.write('\nFinished wait_for_any_response() at %s\n' % datetime.now())

    def is_prompt_bad(self, prompt_output):
        return len(prompt_output) == 0 or prompt_output == CRNL

    def are_prompts_similar(self, prompt_one, prompt_two):
        if self.is_prompt_bad(prompt_one) or self.is_prompt_bad(prompt_two):
            if DEBUG_VERBOSE_PRINTING:
                sys.stderr.write('\n[A prompt was bad: ]')
                sys.stderr.write('\n[first prompt: {0!r}]'.format(prompt_one))
                sys.stderr.write('\n[second prompt: {0!r}]'.format(prompt_two))
            return False

        if len(prompt_one) == 0:
            return False  # it will be used as the denominator of a ratio
        lev_dist = self.levenshtein_distance(prompt_one, prompt_two)
        lev_ratio = float(lev_dist) / len(prompt_one)

        if lev_ratio < 0.4:
            return True
        else:
            if DEBUG_VERBOSE_PRINTING:
                sys.stderr.write('\n[! distance too far: \n{0!r}\n{1!r}]\n'.format(prompt_one, prompt_two))
                sys.stderr.write('\n[val=%f, ld=%i]\n' % (lev_ratio, lev_dist))

        return False

