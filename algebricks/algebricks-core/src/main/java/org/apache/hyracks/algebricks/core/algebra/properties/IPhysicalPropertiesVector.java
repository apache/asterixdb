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
package edu.uci.ics.hyracks.algebricks.core.algebra.properties;

import java.util.List;
import java.util.Map;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.EquivalenceClass;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;

public interface IPhysicalPropertiesVector {

    public IPartitioningProperty getPartitioningProperty();

    public List<ILocalStructuralProperty> getLocalProperties();

    /**
     * 
     * @param reqd
     *            vector of required properties
     * @param equivalenceClasses
     * @param fds
     * @return a vector of properties from pvector that are not delivered by the
     *         current vector or null if none
     */
    public IPhysicalPropertiesVector getUnsatisfiedPropertiesFrom(IPhysicalPropertiesVector reqd,
            boolean mayExpandProperties, Map<LogicalVariable, EquivalenceClass> equivalenceClasses,
            List<FunctionalDependency> fds);

    public IPhysicalPropertiesVector clone();

}
