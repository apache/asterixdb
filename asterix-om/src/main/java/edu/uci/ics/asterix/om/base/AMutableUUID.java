package edu.uci.ics.asterix.om.base;

import java.util.UUID;

public class AMutableUUID extends AUUID {

    public AMutableUUID(UUID uuid) {
        super(uuid);
    }

    public void setValue(UUID uuid) {
        this.uuid = uuid;
    }

}
