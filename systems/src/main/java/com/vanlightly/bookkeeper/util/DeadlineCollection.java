package com.vanlightly.bookkeeper.util;

import java.util.*;
import java.util.stream.Collectors;

/*
    A collection that orders items according to a deadline.
    Used for things like timeouts and delays.
 */
public class DeadlineCollection<T> {
    NavigableMap<Long, Queue<T>> data;

    public DeadlineCollection() {
        this.data = new TreeMap<>();
    }

    public void add(long deadline, T t) {
        data.compute(deadline, (k, v) -> {
            if (v == null) {
                v =  new ArrayDeque<>();
            }
            v.add(t);
            return v;
        });
    }

    public boolean hasNext(long deadline) {
        if (data.isEmpty()) {
            return false;
        }

        Long firstKey = data.firstKey();

        return firstKey != null && firstKey <= deadline;
    }

    public T next() {
        Map.Entry<Long, Queue<T>> nextItems = data.firstEntry();
        T item = nextItems.getValue().poll();

        if (nextItems.getValue().isEmpty()) {
            data.remove(nextItems.getKey());
        }

        return item;
    }

    public List<T> getAll() {
        return data.entrySet().stream()
                .flatMap(x -> x.getValue().stream())
                .collect(Collectors.toList());
    }

    public void clear() {
        data.clear();
    }
}
