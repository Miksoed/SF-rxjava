package com.example.rx.scheduler;

public interface Scheduler {

    void execute(Runnable task);

    void shutdown();
}