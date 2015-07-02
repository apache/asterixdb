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
package org.apache.hyracks.api.client.impl;

import java.util.HashSet;
import java.util.Set;

import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.OperatorDescriptorId;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.JobSpecification;

public class PlanUtils {
    public static void visit(JobSpecification spec, IOperatorDescriptorVisitor visitor) throws HyracksException {
        Set<OperatorDescriptorId> seen = new HashSet<OperatorDescriptorId>();
        for (IOperatorDescriptor op : spec.getOperatorMap().values()) {
            visitOperator(visitor, seen, op);
        }
    }

    private static void visitOperator(IOperatorDescriptorVisitor visitor, Set<OperatorDescriptorId> seen,
            IOperatorDescriptor op) throws HyracksException {
        if (!seen.contains(op)) {
            visitor.visit(op);
        }
        seen.add(op.getOperatorId());
    }

    public static void visit(JobSpecification spec, IConnectorDescriptorVisitor visitor) throws HyracksException {
        for (IConnectorDescriptor c : spec.getConnectorMap().values()) {
            visitor.visit(c);
        }
    }
}