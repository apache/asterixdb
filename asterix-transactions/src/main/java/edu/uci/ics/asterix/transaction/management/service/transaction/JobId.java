package edu.uci.ics.asterix.transaction.management.service.transaction;

import java.io.Serializable;

public class JobId implements Serializable {
    private static final long serialVersionUID = 1L;

    private int id;

    public JobId(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    @Override
    public int hashCode() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof JobId)) {
            return false;
        }
        return ((JobId) o).id == id;
    }

    @Override
    public String toString() {
        return "JID:" + id;
    }

	public void setId(int jobId) {
		id = jobId;
	}
}