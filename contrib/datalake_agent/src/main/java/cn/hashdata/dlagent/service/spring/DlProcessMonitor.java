package cn.hashdata.dlagent.service.spring;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Component
public class DlProcessMonitor {
    private static final Logger log = LoggerFactory.getLogger(DlProcessMonitor.class);
    private static boolean destroy = false;

    @Value("${parent.pid}")
    String parentPid;

    @PreDestroy
    public void destroy() {
        destroy = true;
    }

    @Scheduled(fixedRate = 1000)
    public void run() {
        if (destroy) {
            return;
        }

        Path processDirectory = Paths.get("/proc/" + parentPid);
        if (!Files.exists(processDirectory)) {
            log.info("exiting dlproxy due to parent process directory not exist");
            System.exit(0);
        }

        String filePath = "/proc/" + parentPid + "/cmdline";
        File file = new File(filePath);
        if (!file.exists()) {
            log.info("exiting dlproxy due to cmdline file of parent process not exist");
            System.exit(0);
        }

        boolean found = false;
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String cmdLine = br.readLine();
            if (cmdLine.contains("datalake proxy")) {
                found = true;
            }
        } catch (Exception e) {
            log.error("failed to read command line file.", e);
        }

        if (!found) {
            log.info("exiting dlproxy due to command line file is corrupted");
            System.exit(0);
        }
    }
}
