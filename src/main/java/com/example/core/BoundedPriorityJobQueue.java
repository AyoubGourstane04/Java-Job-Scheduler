package com.example.core;

import com.example.model.PrioritizedJob;

import java.util.PriorityQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class BoundedPriorityJobQueue  {
    private final PriorityQueue<PrioritizedJob> queue;
    private final int capacity;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();
    private final Condition notFull = lock.newCondition();

    public BoundedPriorityJobQueue(int queueCapacity) throws IllegalArgumentException{
        if(queueCapacity <= 0){
            throw new IllegalArgumentException("Capacity cannot be Negative");
        }

        this.capacity = queueCapacity;
        this.queue = new PriorityQueue<>();
    }

    public boolean offer(PrioritizedJob job){
        lock.lock();
        try {
            if(queue.size() >= capacity){
                return false;
            }

            queue.offer(job);
            notEmpty.signal();
            return true;

        }finally {
            lock.unlock();
        }
    }

    public void put(PrioritizedJob job) throws InterruptedException {
        lock.lockInterruptibly();
        try {
            while (queue.size() >= capacity){
                notFull.await();
            }
            queue.offer(job);
            notEmpty.signal();
        }finally {
            lock.unlock();
        }
    }

    public PrioritizedJob poll(long timeout, TimeUnit unit) throws InterruptedException {
        long nanos = unit.toNanos(timeout);
        lock.lockInterruptibly();

        try{
            while(queue.isEmpty()){
                if(nanos <= 0){
                    return null;
                }

                nanos = notEmpty.awaitNanos(nanos);
            }


            PrioritizedJob job = queue.poll();
            notFull.signal();

            return job;
        }finally {
            lock.unlock();
        }
    }

    public boolean isEmpty(){
        lock.lock();
        try {
            return queue.isEmpty();
        }finally{
            lock.unlock();
        }
    }

    public int size(){
        lock.lock();

        try{
            return queue.size();
        }finally {
            lock.unlock();
        }
    }



}
