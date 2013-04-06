package edu.uci.ics.asterix.transaction.management.service.transaction;

import java.io.Serializable;

public class DatasetId implements Serializable {
    int id;

    public DatasetId(int id) {
        this.id = id;
    }

    public void setId(int id) {
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
        if ((o == null) || !(o instanceof DatasetId)) {
            return false;
        }
        return ((DatasetId) o).id == this.id;
    }
}
