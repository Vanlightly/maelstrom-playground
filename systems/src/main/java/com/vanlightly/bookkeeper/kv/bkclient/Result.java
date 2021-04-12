package com.vanlightly.bookkeeper.kv.bkclient;

public class Result<T> {
    String code;
    T data;

    public Result(String code, T data) {
        this.code = code;
        this.data = data;
    }

    public String getCode() {
        return code;
    }

    public T getData() {
        return data;
    }
}
