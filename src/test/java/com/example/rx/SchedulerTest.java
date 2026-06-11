package com.example.rx;

import com.example.rx.scheduler.SingleThreadScheduler;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class SchedulerTest {

    @Test
    void observeOnShouldSwitchThread()
            throws InterruptedException {

        AtomicReference<String> threadName =
                new AtomicReference<>();

        CountDownLatch latch =
                new CountDownLatch(1);

        Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    emitter.onComplete();
                })
                .observeOn(new SingleThreadScheduler())
                .subscribe(new Observer<Integer>() {

                    @Override
                    public void onNext(Integer item) {
                        threadName.set(
                                Thread.currentThread().getName());
                    }

                    @Override
                    public void onError(Throwable t) {
                    }

                    @Override
                    public void onComplete() {
                        latch.countDown();
                    }
                });

        latch.await(3, TimeUnit.SECONDS);

        assertNotNull(threadName.get());
    }
}