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
package org.apache.hyracks.api.job;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IWritable;

public final class DeployedJobSpecId implements IWritable, Serializable {

    public static final DeployedJobSpecId INVALID = new DeployedJobSpecId(-1l);

    private static final long serialVersionUID = 1L;
    private long id;

    public static DeployedJobSpecId create(DataInput dis) throws IOException {
        DeployedJobSpecId deployedJobSpecId = new DeployedJobSpecId();
        deployedJobSpecId.readFields(dis);
        return deployedJobSpecId;
    }

    private DeployedJobSpecId() {
    }

    public DeployedJobSpecId(long id) {
        this.id = id;
    }

    public long getId() {
        return id;
    }

    @Override
    public int hashCode() {
        return (int) id;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof DeployedJobSpecId)) {
            return false;
        }
        return ((DeployedJobSpecId) o).id == id;
    }

    @Override
    public String toString() {
        return "PDJID:" + id;
    }

    public static DeployedJobSpecId parse(String str) throws HyracksDataException {
        if (str.startsWith("PDJID:")) {
            return new DeployedJobSpecId(Long.parseLong(str.substring(4)));
        }
        throw HyracksDataException.create(ErrorCode.NOT_A_JOBID, str);
    }

    @Override
    public void writeFields(DataOutput output) throws IOException {
        output.writeLong(id);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        id = input.readLong();
    }
}
