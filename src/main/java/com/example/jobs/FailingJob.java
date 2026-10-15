package com.example.jobs;

import com.example.model.Job;

public class FailingJob implements Job {

    private final String id;

    public FailingJob(String id){
        this.id = id;
    }

    @Override
    public String getId() {
        return this.id;
    }



    @Override
    public void execute() throws Exception {
        Thread.currentThread().interrupt();
    }
}
