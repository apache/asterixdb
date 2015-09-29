package org.apache.asterix.common.feeds;

/**
 * A unique identifier for a an active job. There
 * is not always a one-to-one correspondence between active
 * objects and active jobs. In the case of feeds, one feed
 * can have many active jobs, represented by feedconnectionid
 */
public class ActiveJobId {

    protected final ActiveId activeId;

    public ActiveJobId(ActiveId activeId) {
        this.activeId = activeId;
    }

    public ActiveId getActiveId() {
        return activeId;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o instanceof ActiveJobId)) {
            return false;
        }

        if (this == o || ((ActiveJobId) o).getActiveId().equals(activeId)) {
            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public String toString() {
        return activeId.toString();
    }

}