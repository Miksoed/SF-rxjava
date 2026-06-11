package com.example.rx;

import com.example.rx.scheduler.Scheduler;

import java.util.function.Function;
import java.util.function.Predicate;

public class Observable<T> {

    private final ObservableOnSubscribe<T> source;

    private Scheduler subscribeScheduler;
    private Scheduler observeScheduler;

    private Observable(ObservableOnSubscribe<T> source) {
        this.source = source;
    }

    public static <T> Observable<T> create(
            ObservableOnSubscribe<T> source) {
        return new Observable<>(source);
    }

    public Disposable subscribe(Observer<T> observer) {

        SimpleDisposable disposable = new SimpleDisposable();

        Runnable subscriptionTask = () -> {
            try {
                source.subscribe(new Emitter<>() {

                    @Override
                    public void onNext(T item) {

                        if (disposable.isDisposed()) {
                            return;
                        }

                        Runnable task = () ->
                                observer.onNext(item);

                        if (observeScheduler != null) {
                            observeScheduler.execute(task);
                        } else {
                            task.run();
                        }
                    }

                    @Override
                    public void onError(Throwable throwable) {

                        if (disposable.isDisposed()) {
                            return;
                        }

                        Runnable task = () ->
                                observer.onError(throwable);

                        if (observeScheduler != null) {
                            observeScheduler.execute(task);
                        } else {
                            task.run();
                        }
                    }

                    @Override
                    public void onComplete() {

                        if (disposable.isDisposed()) {
                            return;
                        }

                        Runnable task = observer::onComplete;

                        if (observeScheduler != null) {
                            observeScheduler.execute(task);
                        } else {
                            task.run();
                        }
                    }

                    @Override
                    public boolean isDisposed() {
                        return disposable.isDisposed();
                    }
                });
            } catch (Exception e) {
                observer.onError(e);
            }
        };

        if (subscribeScheduler != null) {
            subscribeScheduler.execute(subscriptionTask);
        } else {
            subscriptionTask.run();
        }

        return disposable;
    }

    public <R> Observable<R> map(
            Function<T, R> mapper) {

        return create(emitter ->
                subscribe(new Observer<>() {

                    @Override
                    public void onNext(T item) {
                        try {
                            emitter.onNext(
                                    mapper.apply(item));
                        } catch (Exception e) {
                            emitter.onError(e);
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                        emitter.onError(t);
                    }

                    @Override
                    public void onComplete() {
                        emitter.onComplete();
                    }
                }));
    }

    public Observable<T> filter(
            Predicate<T> predicate) {

        return create(emitter ->
                subscribe(new Observer<>() {

                    @Override
                    public void onNext(T item) {
                        try {
                            if (predicate.test(item)) {
                                emitter.onNext(item);
                            }
                        } catch (Exception e) {
                            emitter.onError(e);
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                        emitter.onError(t);
                    }

                    @Override
                    public void onComplete() {
                        emitter.onComplete();
                    }
                }));
    }

    public <R> Observable<R> flatMap(
            Function<T, Observable<R>> mapper) {

        return create(emitter ->
                subscribe(new Observer<>() {

                    @Override
                    public void onNext(T item) {

                        try {
                            mapper.apply(item)
                                    .subscribe(
                                            new Observer<>() {
                                                @Override
                                                public void onNext(R item) {
                                                    emitter.onNext(item);
                                                }

                                                @Override
                                                public void onError(Throwable t) {
                                                    emitter.onError(t);
                                                }

                                                @Override
                                                public void onComplete() {
                                                }
                                            });
                        } catch (Exception e) {
                            emitter.onError(e);
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                        emitter.onError(t);
                    }

                    @Override
                    public void onComplete() {
                        emitter.onComplete();
                    }
                }));
    }

    public Observable<T> subscribeOn(
            Scheduler scheduler) {

        this.subscribeScheduler = scheduler;
        return this;
    }

    public Observable<T> observeOn(
            Scheduler scheduler) {

        this.observeScheduler = scheduler;
        return this;
    }

    private static class SimpleDisposable
            implements Disposable {

        private volatile boolean disposed;

        @Override
        public void dispose() {
            disposed = true;
        }

        @Override
        public boolean isDisposed() {
            return disposed;
        }
    }
}