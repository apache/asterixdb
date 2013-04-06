package edu.uci.ics.asterix.transaction.management.service.transaction;

public class MutableResourceId{
    long id;

    public MutableResourceId(long id) {
        this.id = id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getId() {
        return id;
    }

    @Override
    public int hashCode() {
        return (int)id;
    }

    @Override
    public boolean equals(Object o) {
        if ((o == null) || !(o instanceof MutableResourceId)) {
            return false;
        }
        return ((MutableResourceId) o).id == this.id;
    }
}
