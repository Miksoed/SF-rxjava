package com.example.rx;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertTrue;

class ErrorHandlingTest {

    @Test
    void shouldCallOnError() {

        AtomicBoolean called =
                new AtomicBoolean(false);

        Observable.<Object>create(emitter -> {
                    throw new RuntimeException(
                            "Test exception");
                })
                .subscribe(new Observer<Object>() {

                    @Override
                    public void onNext(Object item) {
                    }

                    @Override
                    public void onError(Throwable t) {
                        called.set(true);
                    }

                    @Override
                    public void onComplete() {
                    }
                });

        assertTrue(called.get());
    }
}