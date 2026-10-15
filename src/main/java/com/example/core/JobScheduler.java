package com.example.core;

import com.example.model.Job;
import com.example.model.JobStatus;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class JobScheduler {
    private final BlockingQueue<Job> queue;
    private final AtomicInteger completedJobs = new AtomicInteger();
    private final ConcurrentHashMap<String, JobStatus> statuses = new ConcurrentHashMap<>();
    private final Thread[] workers;

    public JobScheduler(int workerCount, int queueCapacity){
        if(workerCount <= 0 || queueCapacity <= 0){
            throw new IllegalArgumentException("worker count and queue capacity must be positive.");
        }

        this.queue = new LinkedBlockingQueue<>(queueCapacity);
        this.workers = new Thread[workerCount];

        for(int i=0; i<workerCount; i++){
            workers[i] = new Thread(this::workerLoop, "worker-" + (i+1));
            workers[i].start();
        }

    }

    public void submitJob(Job job) throws InterruptedException{
        if(statuses.putIfAbsent(job.getId(), JobStatus.PENDING) != null){
            throw new IllegalArgumentException("Duplicate job ID : " + job.getId());
        }

        try{
            queue.put(job);
        }catch (InterruptedException e){
            statuses.remove(job.getId(), JobStatus.PENDING);
            throw e;
        }
    }

    public JobStatus getJobStatus(String jobId){
        return this.statuses.get(jobId);
    }

    public int getCompletedJobs(){
        return completedJobs.get();
    }


    private void workerLoop() {
        while(!Thread.currentThread().isInterrupted()){
            try{
                Job job = queue.take();
                statuses.put(job.getId(), JobStatus.RUNNING);

                try{
                    job.execute();
                    statuses.put(job.getId(), JobStatus.COMPLETED);
                    completedJobs.incrementAndGet();
                } catch (Exception e) {
                    statuses.put(job.getId(), JobStatus.FAILED);
                    System.err.println("Job " + job.getId() + " failed: " + e.getMessage());
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    public void shutdown() throws InterruptedException{
        for(Thread worker : this.workers){
            worker.interrupt();
        }
        for(Thread worker : this.workers){
            worker.join();
        }
    }

}
