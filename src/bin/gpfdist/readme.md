# gpfdist

## start
- gpfdist_init
    - create hash table for sessions
    - parse_command_line
    - init libevent handler
    - http_setup # start the http server
        - set socket | bind | listen
        - set event for accept
    - sit and wait for events

- do_accept # Callback when the listen socket is ready to accept connections
    - accept connection
    - setup_read # set up for callback when socket ready for reading the http request
        - set do_read_request as EV_READ callback

- do_read_request 
    - request_parse_gp_headers -> get xid/cid/sn ...
    - set_transform # setup transform errorfile and fstream options **we create error file for readable transform here**
    - session_attach
        - apr_hash_get by `tid:path`
        - if not craeted we should create new session
            - sessions_cleanup to remove outdated sessions
            - setup fstream options
            - fstream_open # open fstream and subprocess
                - gfile_open
                    - if transform exists
                    - bind fd->transform i/o routines
                    - subprocess_open **we create error file for writable transform here**
    - handle_post_request
        - if final request -> go to request_end directly
        - if data request -> read data from request and do fstream_write
        - if first request -> go to request_end
        - request_end
            - if goto done_processing_request -> **we will send header in request_end**
            - otherwise -> we send header before request_end
            - if errorfile exists, we read from it, and send error file by proto1; after that `send_eof` or `send_errmsg` (which only happened in read path)
            - do session_end or session_detach
                - if all request in this session is detached, we do session_free
                    - in session_free, we will do fstream_close. **For post request, we can always get error message from error file after that.**
                    - destroy session_pool
            - if session_free, we can get error message and bind it to `request_t->ferror`
            - if we handle post request, we **send ferror as header to client**.

## protocol0 write
each completed write contains at least three requests, with different `X-GP-SEQ` and `X-GP-DONE`

For the first request, `X-GP-SEQ` should be 1. `X-GP-DONE` should not exist.
For the second request to nth request, `X-GP-SEQ` should be 2 - N. `X-GP-DONE` should not exist.
For the last final request, `X-GP-DONE` should be 1. And `X-GP-SEQ` should be N.

Multiple segments can send requests at the same time. If the `tid` and path are same, they should be attached to the same session. 
However, if one or more of the segments are hanged, the requests from those segments may not be received by the `gpfdist` server in time. `Gpfdist` server can deallocate the session, and create another session for the same `tid:path` again for the same write from segments not sending first requests in time.

## error report for write transform
If the subprocess failed not quick enough and writing is short, even the subprocess shall fail, the writing will be success due to the apr_file_write mechanism. The root cause is that we set `out` in `apr_procattr_io_set` as `APR_NO_PIPE`. So the error file won't be written in time.
However, after the session is freed, and the `fstream` is closed, we must have done the `apr_proc_wait`. So that we can get error message from error file for sure.
If we find an error exists, we should send the error message back to client in `request_end`.
Also, we should clean up the error files.

If writing is long enough, we can also get error message during write. After `wrote` is failed, we will check the error file and send the error file to client in header if error exists.
The client won't send `DONE` requests anymore since the error has already happened. So the server won't have a chance to clean the session. However, that will not be an issue, because we will finally do the session clean up job by `sessions_cleanup` if we find that the session is idled for a while.

You will find that the cleaning up of an error file will only be done in `session_free`. So in most cases, we will only get error message in the final request in one of the segments. Other cases are writing failures happened. However, we still clean up the error file in `session_free` in those cases.