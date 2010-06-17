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
package edu.uci.ics.hyracks.api.constraints;

public class AbsoluteLocationConstraint extends LocationConstraint {
    private static final long serialVersionUID = 1L;

    private String nodeId;

    public AbsoluteLocationConstraint() {
    }

    public AbsoluteLocationConstraint(String nodeId) {
        this.nodeId = nodeId;
    }

    @Override
    public ConstraintType getConstraintType() {
        return ConstraintType.ABSOLUTE;
    }

    public String getLocationId() {
        return nodeId;
    }

    public void setLocationId(String nodeId) {
        this.nodeId = nodeId;
    }

    public String toString() {
        return getConstraintType() + ":" + nodeId;
    }
}