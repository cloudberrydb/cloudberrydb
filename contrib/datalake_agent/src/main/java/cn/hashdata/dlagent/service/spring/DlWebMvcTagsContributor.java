package cn.hashdata.dlagent.service.spring;

import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import org.apache.commons.lang.StringUtils;
import cn.hashdata.dlagent.service.HttpHeaderDecoder;
import org.springframework.boot.actuate.metrics.web.servlet.WebMvcTagsContributor;
import org.springframework.stereotype.Component;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Custom {@link WebMvcTagsContributor} that adds dlagent specific tags to metrics for Spring MVC (REST endpoints)
 *
 * @return the {@link WebMvcTagsContributor} instance
 */
@Component
public class DlWebMvcTagsContributor implements WebMvcTagsContributor {

    private static final String UNKNOWN_VALUE = "unknown";
    private final HttpHeaderDecoder decoder;

    public DlWebMvcTagsContributor(HttpHeaderDecoder decoder) {
        this.decoder = decoder;
    }

    @Override
    public Iterable<Tag> getTags(HttpServletRequest request, HttpServletResponse response, Object handler, Throwable exception) {
        // default server tag value to "default" if the request is from a dlagent Client
        // if request is not from dlagent client, apply the same tags wth the value "unknown"
        // because the Prometheus Metrics Registry requires a metric to have a consistent set of tags
        boolean encoded = decoder.areHeadersEncoded(request);
        String defaultServer = StringUtils.isNotBlank(request.getHeader("X-GP-USER")) ? "default" : UNKNOWN_VALUE;
        return Tags.empty()
                .and("user", StringUtils.defaultIfBlank(decoder.getHeaderValue("X-GP-USER", request, encoded), UNKNOWN_VALUE))
                .and("segment", StringUtils.defaultIfBlank(decoder.getHeaderValue("X-GP-SEGMENT-ID", request, encoded), UNKNOWN_VALUE))
                .and("profile", StringUtils.defaultIfBlank(decoder.getHeaderValue("X-GP-OPTIONS-PROFILE", request, encoded), UNKNOWN_VALUE))
                .and("server", StringUtils.defaultIfBlank(decoder.getHeaderValue("X-GP-OPTIONS-SERVER", request, encoded), defaultServer));
    }

    @Override
    public Iterable<Tag> getLongRequestTags(HttpServletRequest request, Object handler) {
        return Tags.empty();
    }

}
