package com.example.jobs;

import com.example.model.Job;

public class EmailJob implements Job {

    private final String id;

    public EmailJob(String id){
        this.id = id;
    }

    @Override
    public String getId() {
        return this.id;
    }

    @Override
    public void execute() throws InterruptedException {
        System.out.println(Thread.currentThread().getName() + " executing Job :  " + this.id);

        Thread.sleep(1000);

        System.out.println("Job : " + this.id + " Finished");

    }
}
