package com.vanlightly.bookkeeper.metadata;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class NonLedgerMetadata {
    Versioned<List<Long>> ledgerList;
    Set<String> availableBookies;
    Set<String> availableClients;
    String leaderClient;

    public NonLedgerMetadata() {
        ledgerList = new Versioned<>(new ArrayList<>(), 0);
        availableBookies = new HashSet<>();
        availableClients = new HashSet<>();
        leaderClient = "none";
    }

    public NonLedgerMetadata(Versioned<List<Long>> ledgerList,
                             Set<String> availableBookies,
                             Set<String> availableClients,
                             String leaderClient) {
        this.ledgerList = ledgerList;
        this.availableBookies = availableBookies;
        this.availableClients = availableClients;
        this.leaderClient = leaderClient;
    }

    public Versioned<List<Long>> getLedgerList() {
        return ledgerList;
    }

    public void setLedgerList(Versioned<List<Long>> ledgerList) {
        this.ledgerList = ledgerList;
    }

    public Set<String> getAvailableBookies() {
        return availableBookies;
    }

    public void setAvailableBookies(Set<String> availableBookies) {
        this.availableBookies = availableBookies;
    }

    public Set<String> getAvailableClients() {
        return availableClients;
    }

    public void setAvailableClients(Set<String> availableClients) {
        this.availableClients = availableClients;
    }

    public String getLeaderClient() {
        return leaderClient;
    }

    public void setLeaderClient(String leaderClient) {
        this.leaderClient = leaderClient;
    }
}
