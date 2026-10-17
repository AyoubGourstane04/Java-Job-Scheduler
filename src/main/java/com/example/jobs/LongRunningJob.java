package com.example.jobs;

import com.example.model.Job;

public class LongRunningJob implements Job {
    private final String id;

    public LongRunningJob(String id){
        this.id = id;
    }

    @Override
    public String getId() {
        return this.id;
    }

    @Override
    public void execute() throws InterruptedException {
        System.out.println(Thread.currentThread().getName() + " executing Job :  " + this.id);

        while (true){
            Thread.sleep(500);
//            System.out.println("hhehhhhe");
        }
    }
}
