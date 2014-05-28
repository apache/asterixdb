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
package edu.uci.ics.hyracks.api.dataflow;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import edu.uci.ics.hyracks.api.io.IWritable;

public final class ActivityId implements IWritable, Serializable {
    private static final long serialVersionUID = 1L;
    private OperatorDescriptorId odId;
    private int id;

    public static ActivityId create(DataInput dis) throws IOException {
        ActivityId activityId = new ActivityId();
        activityId.readFields(dis);
        return activityId;
    }

    private ActivityId() {

    }

    public ActivityId(OperatorDescriptorId odId, int id) {
        this.odId = odId;
        this.id = id;
    }

    public OperatorDescriptorId getOperatorDescriptorId() {
        return odId;
    }

    public int getLocalId() {
        return id;
    }

    @Override
    public int hashCode() {
        return (int) (odId.hashCode() + id);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ActivityId)) {
            return false;
        }
        ActivityId other = (ActivityId) o;
        return other.odId.equals(odId) && other.id == id;
    }

    public String toString() {
        return "ANID:" + odId + ":" + id;
    }

    public static ActivityId parse(String str) {
        if (str.startsWith("ANID:")) {
            str = str.substring(5);
            int idIdx = str.lastIndexOf(':');
            return new ActivityId(OperatorDescriptorId.parse(str.substring(0, idIdx)), Integer.parseInt(str
                    .substring(idIdx + 1)));
        }
        throw new IllegalArgumentException("Unable to parse: " + str);
    }

    @Override
    public void writeFields(DataOutput output) throws IOException {
        odId.writeFields(output);
        output.writeInt(id);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        odId = OperatorDescriptorId.create(input);
        id = input.readInt();
    }
}