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

package org.apache.hyracks.api.deployment;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hyracks.api.io.IWritable;

/**
 * The representation of a deployment id
 *
 * @author yingyib
 */
public class DeploymentId implements IWritable, Serializable {
    private static final long serialVersionUID = 1L;

    private String deploymentKey;

    public static DeploymentId create(DataInput dis) throws IOException {
        DeploymentId deploymentId = new DeploymentId();
        deploymentId.readFields(dis);
        return deploymentId;
    }

    private DeploymentId() {

    }

    public DeploymentId(String deploymentKey) {
        this.deploymentKey = deploymentKey;
    }

    public int hashCode() {
        return deploymentKey.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof DeploymentId)) {
            return false;
        }
        return ((DeploymentId) o).deploymentKey.equals(deploymentKey);
    }

    @Override
    public String toString() {
        return deploymentKey;
    }

    @Override
    public void writeFields(DataOutput output) throws IOException {
        output.writeUTF(deploymentKey);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        deploymentKey = input.readUTF();
    }
}
