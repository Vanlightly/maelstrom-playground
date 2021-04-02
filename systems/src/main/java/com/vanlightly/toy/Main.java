package com.vanlightly.toy;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class Main {
    public static void main(String[] args) {
        AtomicBoolean stopSig = new AtomicBoolean();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                stopSig.set(true);
            }
        });

        EchoServer echoServer = new EchoServer(stopSig);
        echoServer.run();
    }
}
