package com.example;

import com.example.core.Worker;
import com.example.jobs.EmailJob;
import com.example.jobs.FailingJob;
import com.example.model.Job;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        BlockingQueue<Job> queue = new LinkedBlockingQueue<>(5);
        AtomicInteger completedJobs = new AtomicInteger();

        long startTime = System.currentTimeMillis();

        Thread worker1 = new Thread(new Worker(queue, completedJobs), "worker-1");
        Thread worker2 = new Thread(new Worker(queue, completedJobs), "worker-2");
        Thread worker3 = new Thread(new Worker(queue, completedJobs), "worker-3");


        worker1.start();
        worker2.start();
        worker3.start();

        for (int i=1; i<=30; i++){
            queue.put(new EmailJob("job-" + i));
        }


        while(completedJobs.get() < 30){
            Thread.sleep(100);
        }

        worker1.interrupt();
        worker2.interrupt();
        worker3.interrupt();

        worker1.join();
        worker2.join();
        worker3.join();

        long endTime = System.currentTimeMillis();

        System.out.println("All jobs finished.");

        long executionTime = endTime - startTime;

        System.out.println("Execution time : " + executionTime + " ms");

    }
}