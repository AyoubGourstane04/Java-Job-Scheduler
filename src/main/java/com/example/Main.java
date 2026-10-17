package com.example;

import com.example.core.JobScheduler;
import com.example.jobs.EmailJob;
import com.example.jobs.FailingJob;
import com.example.jobs.LongRunningJob;
import com.example.model.JobStatus;


public class Main {
    public static void main(String[] args) throws InterruptedException {

        JobScheduler scheduler = new JobScheduler(3, 10, 2);

        scheduler.submitJob(new EmailJob("job-1"));
        scheduler.submitJob(new EmailJob("job-2"));

        scheduler.submitJob(new FailingJob("job-fail"));

        scheduler.submitJob(new LongRunningJob("job-timeout"));

        Thread.sleep(1500);

        scheduler.submitJob(new LongRunningJob("job-cancel"));

        Thread.sleep(1000);

        boolean cancelled = scheduler.cancelJob("job-cancel");
        System.out.println("Cancellation requested: " + cancelled);

        scheduler.shutdown();

        System.out.println("\n========== FINAL STATISTICS ==========");
        System.out.println("Completed jobs: " + scheduler.getCompletedJobs());
        System.out.println("Finished jobs: " + scheduler.getFinishedJobs());
        System.out.println("Scheduler state: " + scheduler.getState());
    }

}