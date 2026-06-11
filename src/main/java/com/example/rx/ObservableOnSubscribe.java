package com.example.rx;

@FunctionalInterface
public interface ObservableOnSubscribe<T> {

    void subscribe(Emitter<T> emitter);
}