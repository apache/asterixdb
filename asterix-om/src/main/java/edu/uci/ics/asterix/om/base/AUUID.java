package edu.uci.ics.asterix.om.base;

import java.util.UUID;

import org.json.JSONException;
import org.json.JSONObject;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.visitors.IOMVisitor;

public class AUUID implements IAObject {

    private final UUID uuid;

    public AUUID(UUID uuid) {
        this.uuid = uuid;
    }

    public UUID getUUIDValue() {
        return uuid;
    }

    @Override
    public JSONObject toJSON() throws JSONException {
        JSONObject json = new JSONObject();
        json.put("AUUID", uuid.toString());
        return json;
    }

    @Override
    public IAType getType() {
        return BuiltinType.AUUID;
    }

    @Override
    public void accept(IOMVisitor visitor) throws AsterixException {
        visitor.visitAUUID(this);
    }

    @Override
    public boolean deepEqual(IAObject obj) {
        if (!(obj instanceof AUUID)) {
            return false;
        }
        AUUID oUUID = (AUUID) obj;
        return uuid.equals(oUUID.uuid);
    }

    @Override
    public int hash() {
        return uuid.hashCode();
    }

    @Override
    public String toString() {
        return "AUUID: {" + uuid + "}";
    }
}
