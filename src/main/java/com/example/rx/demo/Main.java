package com.example.rx.demo;

import com.example.rx.Observable;
import com.example.rx.Observer;
import com.example.rx.scheduler.ComputationScheduler;

public class Main {

    public static void main(String[] args)
            throws InterruptedException {

        Observable<Integer> observable =
                Observable.create(emitter -> {

                    for (int i = 1; i <= 5; i++) {
                        emitter.onNext(i);
                    }

                    emitter.onComplete();
                });

        observable
                .map(x -> x * 10)
                .filter(x -> x > 20)
                .observeOn(new ComputationScheduler())
                .subscribe(new Observer<>() {

                    @Override
                    public void onNext(Integer item) {
                        System.out.println(
                                "Received: " + item +
                                        " Thread: "
                                        + Thread.currentThread()
                                        .getName());
                    }

                    @Override
                    public void onError(Throwable t) {
                        t.printStackTrace();
                    }

                    @Override
                    public void onComplete() {
                        System.out.println("Completed");
                    }
                });

        Thread.sleep(1000);
    }
}