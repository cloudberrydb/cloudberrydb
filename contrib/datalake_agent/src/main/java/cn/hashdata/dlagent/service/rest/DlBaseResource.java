package cn.hashdata.dlagent.service.rest;

import lombok.extern.slf4j.Slf4j;
import cn.hashdata.dlagent.api.model.RequestContext;
import cn.hashdata.dlagent.service.RequestParser;
import cn.hashdata.dlagent.service.controller.DlErrorReporter;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.MultiValueMap;

import javax.servlet.http.HttpServletRequest;

/**
 * Base class for dlagent REST resources that provides unified error logging and wrapping.
 * All exceptions will be first logged with the proper MDC context and then wrapped into a DlRuntimeException
 * so that the ErrorHandler can process them and not re-throw to the container
 * where they would've been logged again, but without the MDC context.
 *
 * @param <T> type of ResponseEntity that a resource will produce.
 */
@Slf4j
public abstract class DlBaseResource<T> extends DlErrorReporter<T> {
    private final RequestParser<MultiValueMap<String, String>> parser;

    /**
     * Creates a new instance of the resource.
     *
     * @param parser      request parser
     */
    protected DlBaseResource(RequestParser<MultiValueMap<String, String>> parser) {
        this.parser = parser;
    }

    /**
     * Parses the incoming httpServletRequest and produces a response, wrapping and logging an error, if any.
     *
     * @param headers            http servlet request headers
     * @param httpServletRequest http servlet request
     * @return response entity to give to container
     */
    protected ResponseEntity<T> processRequest(final MultiValueMap<String, String> headers,
                                               final HttpServletRequest httpServletRequest) {
        // use the request processing algorithm as a lambda for the invoking and error handling logic
        T response = this.invokeWithErrorHandling(
                () -> {
                    RequestContext context = parser.parseRequest(headers);
                    return produceResponse(context, httpServletRequest);
                }
        );

        // return the response entity, if it is StreamingResponseBody, then the response will be streamed asynchronously
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    /**
     * Produces response of type T by processing a given request.
     *
     * @param context dlagent request context
     * @param request HTTP servlet request
     * @return the response that can be placed in the ResponseEntity and given to the container
     * @throws Exception if operation fails
     */
    protected abstract T produceResponse(RequestContext context, HttpServletRequest request) throws Exception;
}
