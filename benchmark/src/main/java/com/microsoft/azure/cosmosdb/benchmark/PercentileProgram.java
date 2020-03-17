package com.microsoft.azure.cosmosdb.benchmark;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PercentileProgram {

    private static final Logger log = LoggerFactory.getLogger(PercentileProgram.class);

    private static List<Long> latencies = new ArrayList<>();

    public static void main(String[] args) throws IOException {
        readLatencies(args);
    }

    private static void readLatencies(String[] args) throws IOException {
        String fileName = "read_latency.log";
        File file = null;
        if (args.length > 2) {
            fileName = args[2];
            file = new File(fileName);
        } else {
            ClassLoader classLoader = TestingMain.class.getClassLoader();
            URL resource = classLoader.getResource(fileName);
            assert resource != null;
            file = new File(resource.getFile());
        }
        //File is found
        log.info("File Found : " + file.exists());

        //Read File Content
        String content = new String(Files.readAllBytes(file.toPath()));

        String[] split = content.split("\n");
        for(String line : split) {
            if (line.contains("Read Time taken")) {
                String[] read_time_takens = line.split("Read Time taken");
                Long latency = Long.valueOf(read_time_takens[read_time_takens.length - 1].trim());
                latencies.add(latency);
            }
        }
        Collections.sort(latencies);
        log.info("50 percentile {} ", calculatePercentile(latencies, 50));
        log.info("75 percentile {} ", calculatePercentile(latencies, 75));
        log.info("90 percentile {} ", calculatePercentile(latencies, 90));
        log.info("95 percentile {} ", calculatePercentile(latencies, 95));
        log.info("99 percentile {} ", calculatePercentile(latencies, 99));
        log.info("99.9 percentile {} ", calculatePercentile(latencies, 99.9));
    }

    public static long calculatePercentile(List<Long> latencies, double percentile) {
        int index = (int)Math.ceil((percentile / (double)100) * (double)latencies.size());
        return latencies.get(index - 1);
    }
}
