/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.om.base;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.visitors.IOMVisitor;

public final class ABitArray implements IAObject {

    private int numberOfBits;
    private int[] intArray;

    public ABitArray(int nBits) {
        numberOfBits = nBits;
        intArray = new int[nBits / 32 + 1];
    }

    public int[] getIntArray() {
        return intArray;
    }

    public final boolean get(int index) {
        int r = index % 32;
        int q = index / 32;
        int p = 1 << r;
        return (intArray[q] & p) != 0;
    }

    public final int numberOfBits() {
        return this.numberOfBits;
    }

    public final void set(int index, boolean value) {
        int r = index % 32;
        int q = index / 32;
        int p = 1 << r;
        if (value) {
            intArray[q] |= p;
        } else {
            p = ~p;
            intArray[q] &= p;
        }
    }

    public final void setBit(int index) {
        set(index, true);
    }

    public IAType getType() {
        return BuiltinType.ABITARRAY;
    }

    public void or(ABitArray bitArray) {
        int n2 = bitArray.numberOfBits();
        int[] a2 = bitArray.getIntArray();
        int q = n2 / 32;
        for (int i = 0; i < q; i++) {
            this.intArray[i] |= a2[i];
        }
        int r = n2 % 32;
        if (r > 0) {
            int mask = 0;
            for (int i = 0; i < r; i++) {
                mask = (mask << 1) + 1;
            }
            this.intArray[q] |= a2[q] & mask;
        }
    }

    @Override
    public int hashCode() {
        int h = 0;
        for (int i = 0; i < intArray.length; i++) {
            h = h * 31 + intArray[i];
        }
        return h;
    }

    public void accept(IOMVisitor visitor) throws AsterixException {
        visitor.visitABitArray(this);
    }

    @Override
    public boolean deepEqual(IAObject obj) {
        if (!(obj instanceof ABitArray)) {
            return false;
        }
        int[] x = ((ABitArray) obj).getIntArray();
        if (intArray.length != x.length) {
            return false;
        }
        for (int k = 0; k < intArray.length; k++) {
            if (intArray[k] != x[k]) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hash() {
        return hashCode();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ABitArray: [ ");
        for (int i = 0; i < intArray.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(intArray[i]);
        }
        sb.append(" ]");
        return sb.toString();
    }

    @Override
    public JSONObject toJSON() throws JSONException {
        JSONObject json = new JSONObject();

        JSONArray bitArray = new JSONArray();
        for (int i = 0; i < intArray.length; i++) {
            bitArray.put(intArray[i]);
        }
        json.put("ABitArray", bitArray);

        return json;
    }
}
