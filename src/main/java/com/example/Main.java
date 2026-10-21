package com.example;

import com.example.core.JobScheduler;
import com.example.jobs.BlockingJob;
import com.example.jobs.EmailJob;
import com.example.jobs.FailingJob;
import com.example.jobs.LongRunningJob;
import com.example.model.JobPriority;
import com.example.model.JobStatus;

import java.util.concurrent.CountDownLatch;


public class Main {
    public static void main(String[] args) throws InterruptedException {

        JobScheduler scheduler = new JobScheduler(1, 10, 2);

        scheduler.submitJob(new EmailJob("job-low-1"), JobPriority.LOW);
        scheduler.submitJob(new EmailJob("job-low-2"), JobPriority.LOW);
        scheduler.submitJob(new EmailJob("job-high-1"), JobPriority.HIGH);
        scheduler.submitJob(new EmailJob("job-medium-1"), JobPriority.MEDIUM);
        scheduler.submitJob(new FailingJob("job-fail"), JobPriority.MEDIUM);
        scheduler.submitJob(new LongRunningJob("job-cancel"), JobPriority.HIGH);
        scheduler.submitJob(new BlockingJob("job-block", new CountDownLatch(1), new CountDownLatch(1)), JobPriority.MEDIUM);

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