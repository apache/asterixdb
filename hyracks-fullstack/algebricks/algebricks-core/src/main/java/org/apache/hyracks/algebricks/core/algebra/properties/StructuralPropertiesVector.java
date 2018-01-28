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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hyracks.algebricks.core.algebra.base.EquivalenceClass;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;

public class StructuralPropertiesVector implements IPhysicalPropertiesVector {
    private List<ILocalStructuralProperty> propsLocal;
    private IPartitioningProperty propPartitioning;

    public static final StructuralPropertiesVector EMPTY_PROPERTIES_VECTOR =
            new StructuralPropertiesVector(null, new ArrayList<ILocalStructuralProperty>());

    public StructuralPropertiesVector(IPartitioningProperty propPartitioning,
            List<ILocalStructuralProperty> propsLocal) {
        this.propPartitioning = propPartitioning;
        this.propsLocal = propsLocal;
    }

    @Override
    public String toString() {
        return "propsLocal=" + propsLocal + "\tpropPartioning=" + propPartitioning;
    }

    @Override
    public IPartitioningProperty getPartitioningProperty() {
        return propPartitioning;
    }

    @Override
    public List<ILocalStructuralProperty> getLocalProperties() {
        return propsLocal;
    }

    @Override
    public IPhysicalPropertiesVector clone() {
        List<ILocalStructuralProperty> propsCopy = new LinkedList<>();
        if (propsLocal != null) {
            propsCopy.addAll(propsLocal);
        }
        return new StructuralPropertiesVector(propPartitioning, propsCopy);
    }

    /**
     * @param reqd
     *            vector of required properties
     * @return a vector of properties from pvector that are not delivered by the
     *         current vector or null if none
     */
    @Override
    public IPhysicalPropertiesVector getUnsatisfiedPropertiesFrom(IPhysicalPropertiesVector reqd,
            boolean mayExpandProperties, Map<LogicalVariable, EquivalenceClass> equivalenceClasses,
            List<FunctionalDependency> fds) {
        List<ILocalStructuralProperty> plist = reqd.getLocalProperties();
        List<ILocalStructuralProperty> diffLocals = null;
        if (plist != null && !plist.isEmpty()
                && !PropertiesUtil.matchLocalProperties(plist, propsLocal, equivalenceClasses, fds)) {
            diffLocals = plist;
        }

        IPartitioningProperty diffPart = null;
        IPartitioningProperty reqdPart = reqd.getPartitioningProperty();

        if (reqdPart != null) {
            IPartitioningProperty normalizedReqPart =
                    reqdPart.normalize(equivalenceClasses, mayExpandProperties ? fds : null);
            IPartitioningProperty normalizedPropPart =
                    propPartitioning.normalize(equivalenceClasses, mayExpandProperties ? fds : null);
            if (!PropertiesUtil.matchPartitioningProps(normalizedReqPart, normalizedPropPart, mayExpandProperties)) {
                diffPart = reqdPart;
            }
        }

        if (diffLocals == null && diffPart == null) {
            return null;
        } else {
            return new StructuralPropertiesVector(diffPart, diffLocals);
        }
    }

}
