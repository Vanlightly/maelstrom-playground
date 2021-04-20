package com.vanlightly.bookkeeper.utils;

import com.vanlightly.bookkeeper.Commands;
import com.vanlightly.bookkeeper.network.NetworkIO;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class TestNetworkIO implements NetworkIO {
    private String nodeId;
    private Queue<String> inQueue;
    private BlockingQueue<String> outQueue;
    private boolean loseIncoming;
    private boolean loseOutgoing;

    public TestNetworkIO(String nodeId, BlockingQueue<String> outQueue) {
        this.nodeId = nodeId;
        this.outQueue = outQueue;
        this.inQueue = new ArrayDeque<>();
    }

    public boolean losesIncoming() {
        return loseIncoming;
    }

    public void setLoseIncoming(boolean loseIncoming) {
        this.loseIncoming = loseIncoming;
    }

    public boolean losesOutgoing() {
        return loseOutgoing;
    }

    public void setLoseOutgoing(boolean loseOutgoing) {
        this.loseOutgoing = loseOutgoing;
    }

    public void loseAll() {
        loseIncoming = true;
        loseOutgoing = true;
    }

    public void loseNone() {
        loseIncoming = false;
        loseOutgoing = false;
    }

    public void route(String msg) {
        if (!loseIncoming || msg.contains(Commands.PRINT_STATE)) {
            inQueue.add(msg);
        } else if (msg.contains("c1")){
            System.out.println("Dropped msg from c1 to node " + nodeId + " Msg: " + msg);
        }
    }

    @Override
    public boolean hasNext() {
        return !inQueue.isEmpty();
    }

    @Override
    public String readNext() {
        return inQueue.poll();
    }

    @Override
    public void write(String msg) {
        if (!loseOutgoing) {
            outQueue.add(msg);
        }
    }
}
