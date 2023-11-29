package cn.hashdata.dlagent.service;

import org.apache.hadoop.security.DlUserGroupInformation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.net.URL;
import java.net.URLClassLoader;

/**
 * Main Agent Spring Configuration class.
 */
@SpringBootApplication(scanBasePackages = "cn.hashdata.dlagent", scanBasePackageClasses = DlUserGroupInformation.class)
@EnableScheduling
public class ServiceApplication {

    private static final Logger LOG = LoggerFactory.getLogger(ServiceApplication.class);

    /**
     * Constructs a new ServiceApplication
     */
    public ServiceApplication() {
        logClassLoaderInfo();
    }

    /**
     * Logs, at info level, all the libraries loaded by the ClassLoader used by
     * the ServiceApplication.
     */
    private void logClassLoaderInfo() {
        ClassLoader loader = this.getClass().getClassLoader();
        if (loader instanceof URLClassLoader) {
            URLClassLoader urlClassLoader = (URLClassLoader) loader;
            URL[] urls = urlClassLoader.getURLs();
            if (urls != null) {
                for (URL url : urls) {
                    LOG.info("Added repository {}", url);
                }
            }
        }
    }

    /**
     * Spring Boot Main.
     *
     * @param args program arguments
     */
    public static void main(String[] args) {
        SpringApplication.run(ServiceApplication.class, args);
    }

}
