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
package edu.uci.ics.hyracks.api.dataset;

import java.io.Serializable;

import edu.uci.ics.hyracks.api.comm.NetworkAddress;

public class DatasetDirectoryRecord implements Serializable {
    private static final long serialVersionUID = 1L;

    private NetworkAddress address;

    private boolean eos;

    public DatasetDirectoryRecord() {
        this.address = null;
        this.eos = false;
    }

    public void setNetworkAddress(NetworkAddress address) {
        this.address = address;
    }

    public NetworkAddress getNetworkAddress() {
        return address;
    }

    public void setEOS(boolean eos) {
        this.eos = eos;
    }

    public boolean getEOS() {
        return eos;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof DatasetDirectoryRecord)) {
            return false;
        }
        return address.equals(((DatasetDirectoryRecord) o).address);
    }
}