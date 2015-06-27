package edu.uci.ics.asterix.common.feeds.api;

public interface IFrameEventCallback {

    public enum FrameEvent {
        FINISHED_PROCESSING,
        PENDING_WORK_THRESHOLD_REACHED,
        PENDING_WORK_DONE,
        NO_OP,
        FINISHED_PROCESSING_SPILLAGE
    }

    public void frameEvent(FrameEvent frameEvent);
}
