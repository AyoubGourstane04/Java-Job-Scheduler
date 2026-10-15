package com.example.core;

import com.example.model.Job;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class Worker implements Runnable{
    private final BlockingQueue<Job> queue;
    private AtomicInteger jobsCompleted;

    public Worker(BlockingQueue<Job> queue, AtomicInteger jobsCompleted){
        this.queue = queue;
        this.jobsCompleted = jobsCompleted;
    }

    @Override
    public void run() {
        while(!Thread.currentThread().isInterrupted()){
            try{
                Job job = this.queue.take();

                System.out.println(Thread.currentThread().getName() + " picked up " + job.getId());

                job.execute();
                jobsCompleted.incrementAndGet();

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                System.err.println(
                        "Thread " + Thread.currentThread().getName() + " failed: " + e.getMessage()
                );
            }
        }
    }
}
