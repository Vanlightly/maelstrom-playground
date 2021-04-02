package com.vanlightly.bookkeeper.network;

import java.util.Scanner;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class StdInOutNetwork implements NetworkIO {

    Scanner scanner;
    private Lock lock;

    public StdInOutNetwork() {
        scanner = new Scanner(System.in);
        lock = new ReentrantLock();
    }


    @Override
    public boolean hasNext() {
        return scanner.hasNext();
    }

    @Override
    public String readNext() {
        return scanner.nextLine();
    }

    @Override
    public void write(String msg) {
        lock.lock();
        try {
            System.out.println(msg);
            System.out.flush();
        } finally {
            lock.unlock();
        }
    }
}
