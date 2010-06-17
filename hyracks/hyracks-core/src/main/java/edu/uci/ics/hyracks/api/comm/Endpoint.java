/*
 * Copyright 2009-2010 by The Regents of the University of California
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
import java.util.UUID;

public final class Endpoint implements Serializable {
    private static final long serialVersionUID = 1L;

    private final UUID id;

    private final NetworkAddress address;

    private final int receiverIndex;

    public Endpoint(NetworkAddress address, int receiverIndex) throws Exception {
        id = UUID.randomUUID();
        this.address = address;
        this.receiverIndex = receiverIndex;
    }

    public UUID getEndpointId() {
        return id;
    }

    public NetworkAddress getNetworkAddress() {
        return address;
    }

    public int getReceiverIndex() {
        return receiverIndex;
    }

    @Override
    public int hashCode() {
        return id.hashCode() + address.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Endpoint)) {
            return false;
        }
        Endpoint oe = (Endpoint) o;
        return oe.id.equals(id) && oe.address.equals(address);
    }

    @Override
    public String toString() {
        return "[" + address + ":" + id + "]";
    }
}