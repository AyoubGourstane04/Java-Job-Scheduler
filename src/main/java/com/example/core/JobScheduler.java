package com.example.core;

import com.example.model.*;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class JobScheduler {
    private final BlockingQueue<Job> queue;
    private final ConcurrentHashMap<String, JobStatus> statuses = new ConcurrentHashMap<>();
    private final AtomicInteger completedJobs = new AtomicInteger();
    private final AtomicInteger finishedJobs = new AtomicInteger();
    private final AtomicReference<SchedulerState> state = new AtomicReference<>(SchedulerState.RUNNING);
    private final Thread[] workers;
    private final Object submissionLock = new Object();
    private final int maxRetries;
    private final Map<String, Integer> attempts = new ConcurrentHashMap<>();
    private final AtomicInteger oustandingJobs = new AtomicInteger(0);
    private final ScheduledExecutorService retryScheduler = Executors.newSingleThreadScheduledExecutor();
    private final Map<String, Thread> runningThreads = new ConcurrentHashMap<>();
    private final Set<String> cancellationRequests = ConcurrentHashMap.newKeySet();
    private final Set<String> timedOutJobs = ConcurrentHashMap.newKeySet();
    private final Map<String, ScheduledFuture<?>> timeoutTasks = new ConcurrentHashMap<>();
    private final long jobTimeout = 5;
    private final Set<String> finalizedJobs = ConcurrentHashMap.newKeySet();
    private final AtomicLong sequenceCount = new AtomicLong(0);


    public JobScheduler(int workerCount, int queueCapacity, int maxRetries){
        if(workerCount <= 0){
            throw new IllegalArgumentException("worker count must be positive.");
        }

        if(queueCapacity <= 0){
            throw new IllegalArgumentException("Queue capacity must be positive.");
        }

        if(maxRetries < 0){
            throw new IllegalArgumentException("Maximum retries must be positive.");
        }

        this.queue = new LinkedBlockingQueue<>(queueCapacity);
        this.workers = new Thread[workerCount];
        this.maxRetries = maxRetries;

        for(int i=0; i<workerCount; i++){
            workers[i] = new Thread(this::workerLoop, "worker-" + (i+1));
            workers[i].start();
        }

    }

    public void submitJob(Job job) throws InterruptedException{
       synchronized (submissionLock){
           if(state.get() != SchedulerState.RUNNING){
               throw new IllegalArgumentException("Scheduler is shutting down");
           }

           if(statuses.putIfAbsent(job.getId(), JobStatus.PENDING) != null){
               throw new IllegalArgumentException("Duplicate job ID : " + job.getId());
           }

           this.oustandingJobs.incrementAndGet();

           try{
               queue.put(job);
           }catch (InterruptedException e){
               this.oustandingJobs.decrementAndGet();
               statuses.remove(job.getId(), JobStatus.PENDING);
               throw e;
           }
       }

    }

    public void submitJob(Job job, JobPriority priority) throws InterruptedException{
        long sequenceNbr = this.sequenceCount.incrementAndGet();

        PrioritizedJob prioritizedJob = new PrioritizedJob(job, priority, sequenceNbr);

        synchronized (submissionLock){
            if(state.get() != SchedulerState.RUNNING){
                throw new IllegalArgumentException("Scheduler is shutting down");
            }

            if(statuses.putIfAbsent(job.getId(), JobStatus.PENDING) != null){
                throw new IllegalArgumentException("Duplicate job ID : " + job.getId());
            }

            this.oustandingJobs.incrementAndGet();

            try{
                queue.put(job);
            }catch (InterruptedException e){
                this.oustandingJobs.decrementAndGet();
                statuses.remove(job.getId(), JobStatus.PENDING);
                throw e;
            }
        }

    }

    public JobStatus getJobStatus(String jobId){
        return this.statuses.get(jobId);
    }

    public SchedulerState getState() {
        return state.get();
    }

    public int getCompletedJobs(){
        return completedJobs.get();
    }

    public int getFinishedJobs(){
        return finishedJobs.get();
    }

    private void workerLoop() {
        while(true){
            try{
                Job job = queue.poll(100, TimeUnit.MILLISECONDS);

                if (job == null){
                    if(state.get() == SchedulerState.SHUTTING_DOWN && oustandingJobs.get() == 0){
                        break;
                    }
                    continue;
                }

                String jobId = job.getId();
                Thread worker = Thread.currentThread();

                statuses.put(jobId, JobStatus.RUNNING);

                int attemptNumber = attempts.merge(jobId, 1, Integer::sum);

                System.out.println(" job : " + jobId + " - (attempt " + attemptNumber + ")");

                runningThreads.put(jobId, worker);

                ScheduledFuture<?> timeoutTask = retryScheduler.schedule(() -> {
                   if(runningThreads.get(jobId) == worker){
                       timedOutJobs.add(jobId);
                       worker.interrupt();
                   }
                }, jobTimeout, TimeUnit.SECONDS);

                timeoutTasks.put(jobId, timeoutTask);


                try {
                    job.execute();
                    if(timedOutJobs.remove(jobId)){
                        finalizeJob(jobId, JobStatus.CANCELLED);
                        System.out.println("Job " + jobId + " timed out.");

                    }else if(cancellationRequests.remove(job.getId())){
                        finalizeJob(jobId, JobStatus.CANCELLED);
                    }else{
                        finalizeJob(jobId, JobStatus.COMPLETED);
                    }
                } catch (Exception e) {
                    if(timedOutJobs.remove(jobId)){
                        finalizeJob(jobId, JobStatus.CANCELLED);
                        System.out.println("Job " + jobId + " timed out.");

                    }else if(cancellationRequests.remove(job.getId())){
                        finalizeJob(jobId, JobStatus.CANCELLED);
                    } else {
                        System.err.println(
                                "Job " + job.getId()
                                        + " failed on attempt " + attemptNumber
                                        + ": " + e.getMessage()
                        );

                        if (attemptNumber <= this.maxRetries) {
                            scheduleRetry(job, attemptNumber);
                        } else {
                            System.err.println(
                                    "Job " + job.getId()
                                            + " permanently failed after "
                                            + attemptNumber + " attempts."
                            );
                            finalizeJob(jobId, JobStatus.FAILED);
                        }
                    }
                }finally {
                    runningThreads.remove(job.getId(), Thread.currentThread());
                    ScheduledFuture<?> task = timeoutTasks.remove(jobId);
                    if(task != null){
                        task.cancel(false);
                    }
                }
            }catch (InterruptedException e) {
                if(state.get() == SchedulerState.SHUTTING_DOWN && queue.isEmpty()){
                    break;
                }
            }
        }
    }

    public void shutdown() throws InterruptedException{
        synchronized (submissionLock) {
            if (!state.compareAndSet(SchedulerState.RUNNING, SchedulerState.SHUTTING_DOWN)) {
                if (state.get() == SchedulerState.TERMINATED) {
                    return;
                }
            }
        }

        for(Thread worker : this.workers){
            worker.join();
        }

        retryScheduler.shutdown();

        if(!retryScheduler.awaitTermination(10, TimeUnit.SECONDS)){
            System.err.println(
                    "Warning: retry scheduler did not terminate in time."
            );
        }

        state.set(SchedulerState.TERMINATED);
        System.out.println("Scheduler terminated");


    }

    private void scheduleRetry(Job job, int attemptNumber){
        long delay = Math.min(1L << (attemptNumber - 1), 8L);

        System.out.println(
                "Scheduling retry for job " + job.getId()
                        + " in " + delay + " second(s)"
        );

        retryScheduler.schedule(() -> {
            try {
                queue.put(job);
                statuses.put(job.getId(), JobStatus.PENDING);
            }catch (InterruptedException e){
                Thread.currentThread().interrupt();
                statuses.put(job.getId(), JobStatus.FAILED);
                finishedJobs.incrementAndGet();
                oustandingJobs.decrementAndGet();
                System.err.println(
                        "Could not schedule retry for " + job.getId()
                );
            }
        }, delay, TimeUnit.SECONDS);
    }

    public boolean cancelJob(String jobId){
        Thread worker = runningThreads.get(jobId);

        if(worker == null){
            return false;
        }

        cancellationRequests.add(jobId);
        worker.interrupt();

        return true;
    }

    private void finalizeJob(String jobId, JobStatus finalStatus){
        if(!finalizedJobs.add(jobId)){
            return;
        }
        statuses.put(jobId, finalStatus);

        if(finalStatus == JobStatus.COMPLETED){
            completedJobs.incrementAndGet();
        }

        finishedJobs.incrementAndGet();
        oustandingJobs.decrementAndGet();

        System.out.println(
                "Job " + jobId + " finalized as " + finalStatus
        );
    }

}
