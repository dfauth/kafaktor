package com.github.dfauth.akka.typed.simple.kafka;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

public class Transaction {

    private Supplier<Boolean> onSuccess = () -> true;
    private Function<Throwable,Boolean> onFailure = ignored -> false;

    public <T> void onComplete(CompletableFuture<T> f, Supplier<T> s) {
        this.onSuccess = () -> f.complete(s.get());
        this.onFailure = t -> f.completeExceptionally(t);
    }

    public boolean commit() {
        return onSuccess.get();
    }

    public boolean rollback(Throwable t) {
        return onFailure.apply(t);
    }

    static class Monitor {

        private static ThreadLocal<Transaction> tLocal = ThreadLocal.withInitial(() -> new Transaction());

        static Transaction create() {
            return tLocal.get();
        }

        static Transaction currentTransaction() {
            return tLocal.get();
        }
    }
}
