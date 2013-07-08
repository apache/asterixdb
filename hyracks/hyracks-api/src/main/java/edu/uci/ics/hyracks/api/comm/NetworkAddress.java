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
package edu.uci.ics.hyracks.api.comm;

import java.io.Serializable;
import java.util.Arrays;

public final class NetworkAddress implements Serializable {
    private static final long serialVersionUID = 1L;

    private final byte[] ipAddress;

    private final int port;

    public NetworkAddress(byte[] ipAddress, int port) {
        this.ipAddress = ipAddress;
        this.port = port;
    }

    public byte[] getIpAddress() {
        return ipAddress;
    }

    public int getPort() {
        return port;
    }

    @Override
    public String toString() {
        return Arrays.toString(ipAddress) + ":" + port;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(ipAddress) + port;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof NetworkAddress)) {
            return false;
        }
        NetworkAddress on = (NetworkAddress) o;
        return on.port == port && Arrays.equals(on.ipAddress, ipAddress);
    }
}