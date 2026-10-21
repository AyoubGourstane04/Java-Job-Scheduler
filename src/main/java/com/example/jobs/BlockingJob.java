package com.example.jobs;

import com.example.model.Job;

import java.util.concurrent.CountDownLatch;

public class BlockingJob implements Job {
    private final String id;
    private final CountDownLatch started;
    private final CountDownLatch released;


    public BlockingJob(String id,CountDownLatch started, CountDownLatch released){
        this.id = id;
        this.started = started;
        this.released = released;
    }


    @Override
    public String getId() {
        return this.id;
    }

    @Override
    public void execute() throws Exception {
        started.countDown();
        released.await();
    }
}
