package org.apache.asterix.common.active;

import org.apache.asterix.common.active.ActiveObjectId.ActiveObjectType;

/**
 * A unique identifier for a an active job. There
 * is not always a one-to-one correspondence between active
 * objects and active jobs. In the case of feeds, one feed
 * can have many active jobs, represented by feedconnectionid
 */
public class ActiveJobId {

    protected final ActiveObjectId activeId;

    public ActiveJobId(ActiveObjectId activeId) {
        this.activeId = activeId;
    }

    public ActiveJobId(String dataverse, String name, ActiveObjectType type) {
        this.activeId = new ActiveObjectId(dataverse, name, type);
    }

    public ActiveObjectId getActiveId() {
        return activeId;
    }

    public String getDataverse() {
        return activeId.getDataverse();
    }

    public String getName() {
        return activeId.getName();
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
        return "Job for :" + activeId.toString();
    }

}