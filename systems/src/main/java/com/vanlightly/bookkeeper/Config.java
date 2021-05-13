package com.vanlightly.bookkeeper;

public class Config {
    public final static boolean CheckInvariants = true;
    public final static boolean RecoveryReadsFence = false;
    public final static int AddReadSpreadMs = 0;
    public final static boolean LoseFencingMsg = true;
    public final static double MsgLoss = 0.0;

}
