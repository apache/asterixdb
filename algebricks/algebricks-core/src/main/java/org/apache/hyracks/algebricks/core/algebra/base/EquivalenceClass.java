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
package org.apache.hyracks.algebricks.core.algebra.base;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.hyracks.algebricks.common.utils.ListSet;

public final class EquivalenceClass {
    private Set<ILogicalExpression> expressionMembers = new ListSet<ILogicalExpression>();
    private Set<LogicalVariable> members = new ListSet<LogicalVariable>();
    private ILogicalExpression constRepresentative;
    private LogicalVariable variableRepresentative;
    private boolean representativeIsConst;

    public EquivalenceClass(Collection<LogicalVariable> members, ILogicalExpression constRepresentative) {
        this.members.addAll(members);
        this.constRepresentative = constRepresentative;
        representativeIsConst = true;
    }

    public EquivalenceClass(Collection<LogicalVariable> members, LogicalVariable variableRepresentative) {
        this.members.addAll(members);
        this.variableRepresentative = variableRepresentative;
        representativeIsConst = false;
    }

    public EquivalenceClass(Collection<LogicalVariable> members, ILogicalExpression constRepresentative,
            Collection<ILogicalExpression> expressionMembers) {
        this(members, constRepresentative);
        this.expressionMembers.addAll(expressionMembers);
    }

    public EquivalenceClass(Collection<LogicalVariable> members, LogicalVariable variableRepresentative,
            Collection<ILogicalExpression> expressionMembers) {
        this(members, variableRepresentative);
        this.expressionMembers.addAll(expressionMembers);
    }

    public boolean representativeIsConst() {
        return representativeIsConst;
    }

    public Collection<LogicalVariable> getMembers() {
        return members;
    }

    public Collection<ILogicalExpression> getExpressionMembers() {
        return expressionMembers;
    }

    public boolean contains(LogicalVariable var) {
        return members.contains(var);
    }

    public boolean contains(ILogicalExpression expr) {
        return expressionMembers.contains(expr);
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
        expressionMembers.addAll(ec2.getExpressionMembers());
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
        return "(<" + (representativeIsConst ? constRepresentative : variableRepresentative) + "> " + members + ";"
                + expressionMembers + ")";
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
            if (!expressionMembers.equals(ec.getExpressionMembers())) {
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
