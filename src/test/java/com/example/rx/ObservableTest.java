package com.example.rx;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ObservableTest {

    @Test
    void mapShouldWork() {

        List<Integer> result = new ArrayList<>();

        Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    emitter.onNext(2);
                    emitter.onComplete();
                })
                .map((Integer x) -> x * 2)
                .subscribe(new Observer<Integer>() {

                    @Override
                    public void onNext(Integer item) {
                        result.add(item);
                    }

                    @Override
                    public void onError(Throwable t) {
                        throw new RuntimeException(t);
                    }

                    @Override
                    public void onComplete() {
                    }
                });

        assertEquals(List.of(2, 4), result);
    }

    @Test
    void filterShouldWork() {

        List<Integer> result = new ArrayList<>();

        Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    emitter.onNext(2);
                    emitter.onNext(3);
                    emitter.onComplete();
                })
                .filter((Integer x) -> x > 1)
                .subscribe(new Observer<Integer>() {

                    @Override
                    public void onNext(Integer item) {
                        result.add(item);
                    }

                    @Override
                    public void onError(Throwable t) {
                        throw new RuntimeException(t);
                    }

                    @Override
                    public void onComplete() {
                    }
                });

        assertEquals(List.of(2, 3), result);
    }

    @Test
    void flatMapShouldWork() {

        List<Integer> result = new ArrayList<>();

        Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    emitter.onNext(2);
                    emitter.onComplete();
                })
                .flatMap((Integer x) ->
                        Observable.<Integer>create(e -> {
                            e.onNext(x * 10);
                            e.onComplete();
                        }))
                .subscribe(new Observer<Integer>() {

                    @Override
                    public void onNext(Integer item) {
                        result.add(item);
                    }

                    @Override
                    public void onError(Throwable t) {
                        throw new RuntimeException(t);
                    }

                    @Override
                    public void onComplete() {
                    }
                });

        assertEquals(List.of(10, 20), result);
    }
}