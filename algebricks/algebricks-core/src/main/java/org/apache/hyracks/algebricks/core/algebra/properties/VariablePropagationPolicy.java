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
package org.apache.hyracks.algebricks.core.algebra.properties;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;

public abstract class VariablePropagationPolicy {
    public static final VariablePropagationPolicy ALL = new VariablePropagationPolicy() {
        @Override
        public void propagateVariables(IOperatorSchema target, IOperatorSchema... sources) {
            int n = sources.length;
            for (int i = 0; i < n; i++) {
                target.addAllNewVariables(sources[i]);
            }
        }
    };

    public static final VariablePropagationPolicy NONE = new VariablePropagationPolicy() {
        @Override
        public void propagateVariables(IOperatorSchema target, IOperatorSchema... sources) {
            // do nothing
        }
    };

    /**
     * Adds, from each source, only variables that do not already appear in the
     * target.
     *
     *
     */
    public static final VariablePropagationPolicy ADDNEWVARIABLES = new VariablePropagationPolicy() {
        @Override
        public void propagateVariables(IOperatorSchema target, IOperatorSchema... sources) {
            for (IOperatorSchema s : sources) {
                for (LogicalVariable v : s) {
                    if (target.findVariable(v) < 0) {
                        target.addVariable(v);
                    }
                }
            }
        }
    };

    public abstract void propagateVariables(IOperatorSchema target, IOperatorSchema... sources)
            throws AlgebricksException;

    public static VariablePropagationPolicy concat(final VariablePropagationPolicy... policies) {
        return new VariablePropagationPolicy() {
            @Override
            public void propagateVariables(IOperatorSchema target, IOperatorSchema... sources)
                    throws AlgebricksException {
                if (policies.length != sources.length) {
                    throw new IllegalArgumentException();
                }
                for (int i = 0; i < policies.length; ++i) {
                    VariablePropagationPolicy p = policies[i];
                    IOperatorSchema s = sources[i];
                    p.propagateVariables(target, s);
                }
            }
        };
    }

}
