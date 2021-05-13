package com.vanlightly.bookkeeper.network;

import com.vanlightly.bookkeeper.Config;
import com.vanlightly.bookkeeper.util.LogManager;
import com.vanlightly.bookkeeper.util.Logger;

import java.util.Random;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class StdInOutNetwork implements NetworkIO {

    private Logger logger = LogManager.getLogger(this.getClass().getSimpleName());
    private Scanner scanner;
    private Lock lock;
    private Random rand;


    public StdInOutNetwork() {
        scanner = new Scanner(System.in);
        lock = new ReentrantLock();
        rand = new Random(UUID.randomUUID().hashCode());
    }


    @Override
    public boolean hasNext() {
        return scanner.hasNext();
    }

    @Override
    public String readNext() {
        String line = scanner.nextLine();
        return line;
    }

    @Override
    public void write(String msg) {
        if (Config.MsgLoss > 0) {
            if (rand.nextDouble() <= Config.MsgLoss) {
                logger.logDebug("DROPPED: " + msg);
                return;
            }
        }

        lock.lock();
        try {
            System.out.println(msg);
            System.out.flush();
//            System.err.println(msg);
//            System.err.flush();
        } finally {
            lock.unlock();
        }
    }
}
