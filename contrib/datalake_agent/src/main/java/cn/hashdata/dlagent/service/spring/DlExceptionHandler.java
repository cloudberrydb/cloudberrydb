package cn.hashdata.dlagent.service.spring;

import cn.hashdata.dlagent.api.error.DlRuntimeException;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Handler for dlagent specific exceptions that just reports the request error status. The actual message body with
 * all proper error attributes is created by the Spring MVC BasicErrorController.
 * <p>
 * This handler prevents the dlagent specific exception from being thrown to the container, where it would've gotten
 * logged without an MDC context, since by that time the MDC context is cleaned up.
 * <p>
 * Instead, it is assumed that the dlagent specific exception has been seen by the the dlagent resource
 * or the processing logic and was logged there, where the MDC context is still available.
 */
@ControllerAdvice
public class DlExceptionHandler {

    @ExceptionHandler({DlRuntimeException.class})
    public void handleDlAgentRuntimeException(DlRuntimeException e, HttpServletResponse response) throws IOException {
        response.sendError(HttpStatus.INTERNAL_SERVER_ERROR.value());
    }

}
