package com.example.model;

public interface Job extends Runnable{
    String getId();

    void execute() throws Exception;

    @Override
    default void run(){
        try{
            execute();
        } catch (Exception e) {
            System.err.println(
                    "Job " + getId() + " failed: " + e.getMessage()
            );
        }
    }
}
