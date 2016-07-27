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

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hyracks.algebricks.core.algebra.base.EquivalenceClass;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;

public interface ILocalStructuralProperty extends IStructuralProperty {
    public enum PropertyType {
        LOCAL_GROUPING_PROPERTY,
        LOCAL_ORDER_PROPERTY
    }

    /**
     * Gets the variables that are used in the local property
     *
     * @param variables,
     *            variables that are used in the local property will be added into the argument collection.
     */
    public void getVariables(Collection<LogicalVariable> variables);

    /**
     * Get the type of the local property.
     *
     * @return either LOCAL_GROUPING_PROPERTY or LOCAL_ORDER_PROPERTY.
     */
    public PropertyType getPropertyType();

    /**
     * Returns the retained property regarding to a collection of variables,
     * e.g., some variables used in the property may not exist in the input
     * collection and hence the data property changes.
     *
     * @param vars
     *            , an input collection of variables
     * @return the retained data property.
     */
    public ILocalStructuralProperty retainVariables(Collection<LogicalVariable> vars);

    /**
     * Returns the additional data property within each group, which is dictated by the group keys.
     *
     * @param vars
     *            , group keys.
     * @return the additional data property within each group.
     */
    public ILocalStructuralProperty regardToGroup(Collection<LogicalVariable> groupKeys);

    /**
     * Returns a new, normalized local structural property representation.
     *
     * @param equivalenceClasses,
     *            maps that mapping variables to equivalence classes.
     * @param fds,
     *            a list of functional dependencies.
     * @return a new normalized local structural property.
     */
    public ILocalStructuralProperty normalize(Map<LogicalVariable, EquivalenceClass> equivalenceClasses,
            List<FunctionalDependency> fds);
}
