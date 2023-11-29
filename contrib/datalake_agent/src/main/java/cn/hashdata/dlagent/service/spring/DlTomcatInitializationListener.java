package cn.hashdata.dlagent.service.spring;

import lombok.extern.slf4j.Slf4j;
import org.apache.catalina.connector.Connector;
import org.apache.coyote.ProtocolHandler;
import org.apache.coyote.http11.Http11NioProtocol;
import org.springframework.boot.web.embedded.tomcat.TomcatWebServer;
import org.springframework.boot.web.server.WebServer;
import org.springframework.boot.web.servlet.context.ServletWebServerInitializedEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

/**
 * Logs Tomcat parameters after the Tomcat container has been initialized.
 */
@Component
@Slf4j
public class DlTomcatInitializationListener implements ApplicationListener<ServletWebServerInitializedEvent> {

    @Override
    public void onApplicationEvent(ServletWebServerInitializedEvent event) {
        WebServer server = event.getWebServer();
        if (server instanceof TomcatWebServer) {
            Connector connector = ((TomcatWebServer) server).getTomcat().getConnector();
            log.info("Tomcat Connector   ={}", connector);
            log.info("- protocol         ={}", connector.getProtocol());
            log.info("- scheme           ={}", connector.getScheme());
            log.info("- port             ={}", connector.getPort());
            log.info("- localPort        ={}", connector.getLocalPort());
            log.info("- asyncTimeout     ={}", connector.getAsyncTimeout());
            log.info("- executorName     ={}", connector.getExecutorName());
            log.info("- maxParameterCount={}", connector.getMaxParameterCount());
            log.info("- maxPostSize      ={}", connector.getMaxPostSize());
            log.info("- URIEncoding      ={}", connector.getURIEncoding());
            log.info("- protocolhandler  ={}", connector.getProtocolHandlerClassName());
            ProtocolHandler handler = connector.getProtocolHandler();
            if (handler instanceof Http11NioProtocol) {
                Http11NioProtocol nioHandler = (Http11NioProtocol) handler;
                log.info("    - connectionTimeout     ={}", nioHandler.getConnectionTimeout());
                log.info("    - disableUploadTmeout   ={}", nioHandler.getDisableUploadTimeout());
                log.info("    - connectionUploadTmeout={}", nioHandler.getConnectionUploadTimeout());
                log.info("    - maxHeaderCount        ={}", nioHandler.getMaxHeaderCount());
                log.info("    - maxHttpHeaderSize     ={}", nioHandler.getMaxHttpHeaderSize());
                log.info("    - maxThreads            ={}", nioHandler.getMaxThreads());
                log.info("    - minSpareThreads       ={}", nioHandler.getMinSpareThreads());
            }
        }
    }
}
