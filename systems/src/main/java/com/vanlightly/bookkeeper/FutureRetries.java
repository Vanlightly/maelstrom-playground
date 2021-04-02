package com.vanlightly.bookkeeper;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

public class FutureRetries {
    public static <T> void retryTransient(CompletableFuture<T> callerFuture,
                          Supplier<CompletableFuture<T>> futureOp) {
        if (!callerFuture.isDone()) {
            final CompletableFuture<T> operationFuture = futureOp.get();

            operationFuture.whenComplete((value, throwable) -> {
                if (throwable != null) {
                    throwable = unwrap(throwable);

                    if (throwable instanceof TransientException) {
                        sleepSynchronously(100);
                        retryTransient(callerFuture,
                                futureOp);
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
                                        Supplier<CompletableFuture<T>> futureOp) {
        if (!callerFuture.isDone()) {
            final CompletableFuture<T> operationFuture = futureOp.get();

            operationFuture.whenComplete((value, throwable) -> {
                if (throwable != null) {
                    if (throwable instanceof OperationCancelledException) {
                        callerFuture.completeExceptionally(throwable);
                    } else {
                        sleepSynchronously(100);
                        retryForever(callerFuture, futureOp);
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

    public static <T> CompletableFuture<T> nonRetryableFailedFuture(Throwable t) {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(t);
        return future;
    }

    private static Throwable unwrap(Throwable t) {
        if (t instanceof ExecutionException || t instanceof CompletionException) {
            return unwrap(t.getCause());
        } else {
            return t;
        }
    }

    // trying to keep this single threaded
    private static void sleepSynchronously(int ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
