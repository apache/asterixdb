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

import java.util.Collection;
import java.util.List;
import java.util.Map;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.EquivalenceClass;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;

public interface IPartitioningProperty extends IStructuralProperty {
    public enum PartitioningType {
        UNPARTITIONED, RANDOM, BROADCAST, UNORDERED_PARTITIONED, ORDERED_PARTITIONED
    }

    static final INodeDomain DOMAIN_FOR_UNPARTITIONED_DATA = new INodeDomain() {
        @Override
        public boolean sameAs(INodeDomain domain) {
            return domain == this;
        }

        @Override
        public Integer cardinality() {
            return null;
        }
    };

    public static final IPartitioningProperty UNPARTITIONED = new IPartitioningProperty() {

        @Override
        public PartitioningType getPartitioningType() {
            return PartitioningType.UNPARTITIONED;
        }

        @Override
        public void normalize(Map<LogicalVariable, EquivalenceClass> equivalenceClasses, List<FunctionalDependency> fds) {
            // do nothing
        }

        @Override
        public void getColumns(Collection<LogicalVariable> columns) {
        }

        @Override
        public INodeDomain getNodeDomain() {
            return DOMAIN_FOR_UNPARTITIONED_DATA;
        }

        @Override
        public String toString() {
            return getPartitioningType().toString();
        }

        @Override
        public void setNodeDomain(INodeDomain domain) {
            throw new IllegalStateException();
        }
    };

    public abstract PartitioningType getPartitioningType();

    public abstract void normalize(Map<LogicalVariable, EquivalenceClass> equivalenceClasses,
            List<FunctionalDependency> fds);

    public abstract INodeDomain getNodeDomain();

    public abstract void setNodeDomain(INodeDomain domain);
}
