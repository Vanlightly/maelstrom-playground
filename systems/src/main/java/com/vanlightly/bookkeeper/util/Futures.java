package com.vanlightly.bookkeeper.util;

import com.vanlightly.bookkeeper.OperationCancelledException;
import com.vanlightly.bookkeeper.TransientException;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;

public class Futures {

    // set from the node to use its delay mechanism
    public static Function<Integer, CompletableFuture<Void>> Delay;

    public static <T> void retryTransient(CompletableFuture<T> callerFuture,
                                          AtomicBoolean isCancelled,
                                          Supplier<CompletableFuture<T>> futureOp) {
        if (!callerFuture.isDone()) {
            final CompletableFuture<T> operationFuture = futureOp.get();

            operationFuture.whenComplete((value, throwable) -> {
                if (isCancelled.get()) {
                    System.out.println("Retry cancelled!!!!");
                    callerFuture.completeExceptionally(new OperationCancelledException());
                }

                if (throwable != null) {
                    throwable = unwrap(throwable);

                    if (throwable instanceof TransientException) {
                        Delay.apply(100).thenRun(() ->
                            retryTransient(callerFuture, isCancelled, futureOp));
                    } else {
                        callerFuture.completeExceptionally(throwable);
                    }
                } else {
                    callerFuture.complete(value);
                }
            });
        }
    }

    public static <T> void retryForever(CompletableFuture<T> callerFuture,
                                        AtomicBoolean isCancelled,
                                        Supplier<CompletableFuture<T>> futureOp) {
        if (!callerFuture.isDone()) {
            final CompletableFuture<T> operationFuture = futureOp.get();

            operationFuture.whenComplete((value, throwable) -> {
                if (isCancelled.get()) {
                    callerFuture.completeExceptionally(new OperationCancelledException());
                }

                if (throwable != null) {
                    if (throwable instanceof OperationCancelledException) {
                        callerFuture.completeExceptionally(throwable);
                    } else {
                        Delay.apply(100).thenRun(() ->
                            retryForever(callerFuture, isCancelled, futureOp));
                    }
                } else {
                    callerFuture.complete(value);
                }
            });
        }
    }

    public static <T> CompletableFuture<T> retryableFailedFuture(String message) {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(new TransientException(message));
        return future;
    }

    public static <T> CompletableFuture<T> failedFuture(Throwable t) {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(t);
        return future;
    }

    public static Throwable unwrap(Throwable t) {
        if (t instanceof ExecutionException || t instanceof CompletionException) {
            return unwrap(t.getCause());
        } else {
            return t;
        }
    }
}
