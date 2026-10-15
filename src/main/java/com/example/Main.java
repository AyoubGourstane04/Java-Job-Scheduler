package com.example;

import com.example.core.JobScheduler;
import com.example.jobs.EmailJob;
import com.example.jobs.FailingJob;
import com.example.model.JobStatus;


public class Main {
    public static void main(String[] args)
            throws Exception {

        JobScheduler scheduler = new JobScheduler(3, 10);

        scheduler.submitJob(new FailingJob("job-fail"));

        for (int i = 1; i <= 5; i++) {
            scheduler.submitJob(new EmailJob("job-" + i));
        }

        while (scheduler.getCompletedJobs() < 6){
            Thread.sleep(50);
        }

        System.out.println("\n--- Final Job Statuses ---");

        System.out.println(
                "job-1: " + scheduler.getJobStatus("job-1")
        );

        System.out.println(
                "job-fail: " + scheduler.getJobStatus("job-fail")
        );

        System.out.println(
                "job-2: " + scheduler.getJobStatus("job-2")
        );

        scheduler.shutdown();
    }

    private static boolean isTerminal(JobStatus status) {
        return status == JobStatus.COMPLETED
                || status == JobStatus.FAILED;
    }
}