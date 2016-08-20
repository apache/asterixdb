/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hyracks.api.dataflow;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hyracks.api.io.IWritable;

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

    public void setOperatorDescriptorId(OperatorDescriptorId odId) {
        this.odId = odId;
    }

    public int getLocalId() {
        return id;
    }

    @Override
    public int hashCode() {
        return odId.hashCode() + id;
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

    @Override
    public String toString() {
        return "ANID:" + odId + ":" + id;
    }

    public static ActivityId parse(String str) {
        if (str.startsWith("ANID:")) {
            str = str.substring(5);
            int idIdx = str.lastIndexOf(':');
            return new ActivityId(OperatorDescriptorId.parse(str.substring(0, idIdx)),
                    Integer.parseInt(str.substring(idIdx + 1)));
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
