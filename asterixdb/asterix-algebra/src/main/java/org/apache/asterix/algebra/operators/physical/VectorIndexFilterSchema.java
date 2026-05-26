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
package org.apache.asterix.algebra.operators.physical;

import java.util.Iterator;
import java.util.Map;

import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;

/**
 * A wrapper schema for vector index filter evaluation that maps variables to
 * their correct physical tuple positions.
 *
 * Vector index physical tuple format (quantized):
 * [distance, qDist, qEmbed, centroidId, pk, include_fields...]
 *
 * The original opSchema only contains [pk] because:
 * - Secondary fields (distance, centroidId, etc.) are internal to the index
 * - INCLUDE fields are only used for filtering, not output
 *
 * This wrapper supports two mapping modes:
 * 1. Variables in opSchema (like pk): position + numSecondaryKeys offset
 * 2. Filter-only variables (INCLUDE fields): direct mapping via filterVarToFieldIndex
 *
 * This allows the TupleFilter to correctly access INCLUDE fields during vector index search
 * without exposing them to downstream operators (which only need pk for primary index lookup).
 */
public class VectorIndexFilterSchema implements IOperatorSchema {

    // Number of internal fields before pk in vector index tuple
    // Quantized: 4 (distance, qDist, qEmbed, centroidId)
    private final int fieldOffset;

    private final IOperatorSchema delegate;

    // Direct mapping for filter-only variables (INCLUDE fields not in output schema)
    // Maps LogicalVariable -> physical tuple field index
    private final Map<LogicalVariable, Integer> filterVarToFieldIndex;

    /**
     * Constructor without direct mapping (backward compatibility).
     * All variables must be in the delegate schema.
     */
    public VectorIndexFilterSchema(IOperatorSchema delegate, int numSecondaryKeys) {
        this(delegate, null, numSecondaryKeys);
    }

    /**
     * Constructor with direct mapping for filter-only variables.
     *
     * @param delegate The operator output schema (contains pk)
     * @param filterVarToFieldIndex Direct mapping for INCLUDE field variables to physical field indexes
     * @param numSecondaryKeys Number of secondary fields before PK (4 for quantized)
     */
    public VectorIndexFilterSchema(IOperatorSchema delegate, Map<LogicalVariable, Integer> filterVarToFieldIndex,
            int numSecondaryKeys) {
        this.delegate = delegate;
        this.filterVarToFieldIndex = filterVarToFieldIndex;
        this.fieldOffset = numSecondaryKeys;
    }

    @Override
    public int findVariable(LogicalVariable var) {
        // First check direct mapping for filter-only variables
        if (filterVarToFieldIndex != null && filterVarToFieldIndex.containsKey(var)) {
            return filterVarToFieldIndex.get(var);
        }

        // Fall back to delegate schema with offset
        int originalPos = delegate.findVariable(var);
        if (originalPos < 0) {
            return originalPos;
        }
        return originalPos + fieldOffset;
    }

    @Override
    public LogicalVariable getVariable(int index) {
        if (index < fieldOffset) {
            // distance and centroidId have no logical variables
            return null;
        }
        return delegate.getVariable(index - fieldOffset);
    }

    @Override
    public int getSize() {
        return delegate.getSize() + fieldOffset;
    }

    @Override
    public void addAllVariables(IOperatorSchema source) {
        delegate.addAllVariables(source);
    }

    @Override
    public void addAllNewVariables(IOperatorSchema source) {
        delegate.addAllNewVariables(source);
    }

    @Override
    public int addVariable(LogicalVariable var) {
        return delegate.addVariable(var) + fieldOffset;
    }

    @Override
    public void clear() {
        delegate.clear();
    }

    @Override
    public Iterator<LogicalVariable> iterator() {
        return delegate.iterator();
    }
}