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
package edu.uci.ics.hyracks.api.dataflow;

import java.io.Serializable;

public final class PortInstanceId implements Serializable {
    public enum Direction {
        INPUT,
        OUTPUT,
    }

    private static final long serialVersionUID = 1L;

    private OperatorDescriptorId odId;
    private Direction direction;
    private int portIndex;
    private int partition;

    public PortInstanceId(OperatorDescriptorId odId, Direction direction, int portIndex, int partition) {
        this.odId = odId;
        this.direction = direction;
        this.portIndex = portIndex;
        this.partition = partition;
    }

    public OperatorDescriptorId getOperatorId() {
        return odId;
    }

    public Direction getDirection() {
        return direction;
    }

    public int getPortIndex() {
        return portIndex;
    }

    public int getPartition() {
        return partition;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((direction == null) ? 0 : direction.hashCode());
        result = prime * result + ((odId == null) ? 0 : odId.hashCode());
        result = prime * result + partition;
        result = prime * result + portIndex;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        PortInstanceId other = (PortInstanceId) obj;
        if (direction == null) {
            if (other.direction != null)
                return false;
        } else if (!direction.equals(other.direction))
            return false;
        if (odId == null) {
            if (other.odId != null)
                return false;
        } else if (!odId.equals(other.odId))
            return false;
        if (partition != other.partition)
            return false;
        if (portIndex != other.portIndex)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return odId + ":" + direction + ":" + partition + ":" + portIndex;
    }
}