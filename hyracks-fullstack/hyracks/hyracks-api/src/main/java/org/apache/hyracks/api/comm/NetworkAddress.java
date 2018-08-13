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
package org.apache.hyracks.api.comm;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.hyracks.api.io.IWritable;

public final class NetworkAddress implements IWritable, Serializable {
    private static final long serialVersionUID = 2L;

    private String address;
    // Cached locally, not serialized
    private byte[] ipAddress;

    private int port;

    public static NetworkAddress create(DataInput dis) throws IOException {
        NetworkAddress networkAddress = new NetworkAddress();
        networkAddress.readFields(dis);
        return networkAddress;
    }

    private NetworkAddress() {
        ipAddress = null;
    }

    public NetworkAddress(String address, int port) {
        this.address = address;
        this.port = port;
        ipAddress = null;
    }

    public String getAddress() {
        return address;
    }

    public byte[] lookupIpAddress() throws UnknownHostException {
        if (ipAddress == null) {
            InetAddress addr = InetAddress.getByName(address);
            ipAddress = addr.getAddress();
        }
        return ipAddress;
    }

    public int getPort() {
        return port;
    }

    @Override
    public String toString() {
        return address + ":" + port;
    }

    @Override
    public int hashCode() {
        return address.hashCode() + port;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof NetworkAddress)) {
            return false;
        }
        NetworkAddress on = (NetworkAddress) o;
        return on.port == port && on.address == address;
    }

    @Override
    public void writeFields(DataOutput output) throws IOException {
        output.writeUTF(address);
        output.writeInt(port);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        address = input.readUTF();
        port = input.readInt();
    }
}
