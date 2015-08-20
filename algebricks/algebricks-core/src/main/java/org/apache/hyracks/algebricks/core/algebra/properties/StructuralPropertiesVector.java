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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.EquivalenceClass;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;

public class StructuralPropertiesVector implements IPhysicalPropertiesVector {
    private List<ILocalStructuralProperty> propsLocal;
    private IPartitioningProperty propPartitioning;

    public static final StructuralPropertiesVector EMPTY_PROPERTIES_VECTOR = new StructuralPropertiesVector(null,
            new ArrayList<ILocalStructuralProperty>());

    public StructuralPropertiesVector(IPartitioningProperty propPartitioning, List<ILocalStructuralProperty> propsLocal) {
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
        List<ILocalStructuralProperty> propsCopy = new LinkedList<ILocalStructuralProperty>();
        if (propsLocal != null) {
            propsCopy.addAll(propsLocal);
        }
        return new StructuralPropertiesVector(propPartitioning, propsCopy);
    }

    /**
     * 
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
        if (plist != null && !plist.isEmpty()) {
            if (!PropertiesUtil.matchLocalProperties(plist, propsLocal, equivalenceClasses, fds)) {
                diffLocals = plist;
            }
        }

        IPartitioningProperty diffPart = null;
        IPartitioningProperty reqdPart = reqd.getPartitioningProperty();
        if (reqdPart != null) {
            if (mayExpandProperties) {
                reqdPart.normalize(equivalenceClasses, fds);
            } else {
                reqdPart.normalize(equivalenceClasses, null);
            }
            propPartitioning.normalize(equivalenceClasses, fds);
            if (!PropertiesUtil.matchPartitioningProps(reqdPart, propPartitioning, mayExpandProperties)) {
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