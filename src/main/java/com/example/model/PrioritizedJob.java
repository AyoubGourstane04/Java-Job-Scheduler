package com.example.model;

public class PrioritizedJob implements Comparable<PrioritizedJob>{
    private final Job job;
    private final JobPriority priority;
    private final long longSequence;

    public PrioritizedJob(Job job, JobPriority priority, long longSequence) {
        this.job = job;
        this.priority = priority;
        this.longSequence = longSequence;
    }

    public Job getJob() {
        return job;
    }

    public JobPriority getPriority() {
        return priority;
    }

    @Override
    public int compareTo(PrioritizedJob other) {
        int priorityComparison = Integer.compare(this.priority.ordinal(), other.priority.ordinal());

        if(priorityComparison != 0){
            return priorityComparison;
        }


        return Long.compare(this.longSequence, other.longSequence);
    }
}
