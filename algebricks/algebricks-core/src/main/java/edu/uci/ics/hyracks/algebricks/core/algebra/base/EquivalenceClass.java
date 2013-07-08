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
package edu.uci.ics.hyracks.algebricks.core.algebra.base;

import java.util.LinkedList;
import java.util.List;

public final class EquivalenceClass {
    private List<LogicalVariable> members;
    private ILogicalExpression constRepresentative;
    private LogicalVariable variableRepresentative;
    private boolean representativeIsConst;

    public EquivalenceClass(List<LogicalVariable> members, ILogicalExpression constRepresentative) {
        this.members = members;
        this.constRepresentative = constRepresentative;
        representativeIsConst = true;
    }

    public EquivalenceClass(List<LogicalVariable> members, LogicalVariable variableRepresentative) {
        this.members = members;
        this.variableRepresentative = variableRepresentative;
        representativeIsConst = false;
    }

    public boolean representativeIsConst() {
        return representativeIsConst;
    }

    public List<LogicalVariable> getMembers() {
        return members;
    }

    public boolean contains(LogicalVariable var) {
        return members.contains(var);
    }

    public ILogicalExpression getConstRepresentative() {
        return constRepresentative;
    }

    public LogicalVariable getVariableRepresentative() {
        return variableRepresentative;
    }

    public void setConstRepresentative(ILogicalExpression constRepresentative) {
        this.constRepresentative = constRepresentative;
        this.representativeIsConst = true;
    }

    public void setVariableRepresentative(LogicalVariable variableRepresentative) {
        this.variableRepresentative = variableRepresentative;
        this.representativeIsConst = false;
    }

    public void merge(EquivalenceClass ec2) {
        members.addAll(ec2.getMembers());
        if (!representativeIsConst && ec2.representativeIsConst()) {
            representativeIsConst = true;
            constRepresentative = ec2.getConstRepresentative();
        }
    }

    public void addMember(LogicalVariable v) {
        members.add(v);
    }

    public EquivalenceClass cloneEquivalenceClass() {
        List<LogicalVariable> membersClone = new LinkedList<LogicalVariable>();
        membersClone.addAll(members);
        EquivalenceClass ec;
        if (representativeIsConst()) {
            ec = new EquivalenceClass(membersClone, constRepresentative);
        } else {
            ec = new EquivalenceClass(membersClone, variableRepresentative);
        }
        return ec;
    }

    @Override
    public String toString() {
        return "(<" + (representativeIsConst ? constRepresentative : variableRepresentative) + "> " + members + ")";
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof EquivalenceClass)) {
            return false;
        } else {
            EquivalenceClass ec = (EquivalenceClass) obj;
            if (!members.equals(ec.getMembers())) {
                return false;
            }
            if (representativeIsConst) {
                return ec.representativeIsConst() && (constRepresentative.equals(ec.getConstRepresentative()));
            } else {
                return !ec.representativeIsConst() && (variableRepresentative.equals(ec.getVariableRepresentative()));
            }
        }
    }
}
