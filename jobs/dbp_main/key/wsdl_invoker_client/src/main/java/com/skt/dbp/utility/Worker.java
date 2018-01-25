package com.skt.dbp.utility;

import com.skt.dbp.wsdl.Client;
import com.skt.dbp.wsdl.GetConvertResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StopWatch;

/**
 * Created by jinchulkim on 2017. 7. 12..
 */
public class Worker {
    private static final Logger logger = LoggerFactory.getLogger(Worker.class);

    private final Client client;
    private String result;
    private long elapsedTime;

    public Worker(final Client client) {
        this.client = client;
    }

    public void run(final String input) throws Exception {
        final StopWatch watch = new StopWatch();
        watch.start();
        final GetConvertResponse response = client.getByInput(input);
        watch.stop();
        result = response.getGetConvertReturn();
        elapsedTime = watch.getTotalTimeMillis();
    }

    public String getResult() {
        return result;
    }

    public long getElapsedTime() {
        return elapsedTime;
    }
}