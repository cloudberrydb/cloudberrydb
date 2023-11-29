package cn.hashdata.dlagent.service.rest;

import cn.hashdata.dlagent.api.model.RequestContext;
import cn.hashdata.dlagent.service.RequestParser;
import cn.hashdata.dlagent.service.controller.ReadService;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import javax.servlet.http.HttpServletRequest;

/**
 * dlagent REST endpoint for read data requests.
 */
@RestController
@RequestMapping("/dlproxy")
public class DlReadResource extends DlBaseResource<StreamingResponseBody> {

    private final ReadService readService;

    /**
     * Creates a new instance of the resource with Request parser and read service implementation.
     *
     * @param parser      http request parser
     * @param readService read service implementation
     */
    public DlReadResource(RequestParser<MultiValueMap<String, String>> parser,
                          ReadService readService) {
        super(parser);
        this.readService = readService;
    }

    /**
     * REST endpoint for read data requests.
     *
     * @param headers http headers from request that carry all parameters
     * @return response object containing stream that will output records
     */
    @GetMapping(value = "/read", produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public ResponseEntity<StreamingResponseBody> read(@RequestHeader MultiValueMap<String, String> headers,
                                                      HttpServletRequest request) {
        return processRequest(headers, request);
    }

    @Override
    protected StreamingResponseBody produceResponse(RequestContext context, HttpServletRequest request) {
        // return a lambda that will be executed asynchronously
        return os -> readService.readData(context, os);
    }
}
