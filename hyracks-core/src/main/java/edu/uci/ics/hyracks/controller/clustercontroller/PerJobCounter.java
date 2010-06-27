package edu.uci.ics.hyracks.controller.clustercontroller;

public class PerJobCounter {
    private int stageCounter;

    public PerJobCounter() {
    }

    public int getStageCounterAndIncrement() {
        return stageCounter++;
    }
}