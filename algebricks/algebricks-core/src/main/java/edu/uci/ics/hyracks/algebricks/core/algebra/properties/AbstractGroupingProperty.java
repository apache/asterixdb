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
import java.util.Set;

import edu.uci.ics.hyracks.algebricks.common.utils.ListSet;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.EquivalenceClass;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;

public abstract class AbstractGroupingProperty {
    protected Set<LogicalVariable> columnSet;

    public AbstractGroupingProperty(Set<LogicalVariable> columnSet) {
        this.columnSet = columnSet;
    }

    public Set<LogicalVariable> getColumnSet() {
        return columnSet;
    }

    public final void normalizeGroupingColumns(Map<LogicalVariable, EquivalenceClass> equivalenceClasses,
            List<FunctionalDependency> fds) {
        replaceGroupingColumnsByEqClasses(equivalenceClasses);
        applyFDsToGroupingColumns(fds);
    }

    private void replaceGroupingColumnsByEqClasses(Map<LogicalVariable, EquivalenceClass> equivalenceClasses) {
        if (equivalenceClasses == null || equivalenceClasses.isEmpty()) {
            return;
        }
        Set<LogicalVariable> norm = new ListSet<LogicalVariable>();
        for (LogicalVariable v : columnSet) {
            EquivalenceClass ec = equivalenceClasses.get(v);
            if (ec == null) {
                norm.add(v);
            } else {
                if (ec.representativeIsConst()) {
                    // trivially satisfied, so the var. can be removed
                } else {
                    norm.add(ec.getVariableRepresentative());
                }
            }
        }
        columnSet = norm;
    }

    private void applyFDsToGroupingColumns(List<FunctionalDependency> fds) {
        // the set of vars. is unordered
        // so we try all FDs on all variables (incomplete algo?)
        if (fds == null || fds.isEmpty()) {
            return;
        }
        Set<LogicalVariable> norm = new ListSet<LogicalVariable>();
        for (LogicalVariable v : columnSet) {
            boolean isImpliedByAnFD = false;
            for (FunctionalDependency fdep : fds) {
                if (columnSet.containsAll(fdep.getHead()) && fdep.getTail().contains(v)) {
                    isImpliedByAnFD = true;
                    norm.addAll(fdep.getHead());
                    break;
                }

            }
            if (!isImpliedByAnFD) {
                norm.add(v);
            }
        }
        columnSet = norm;
    }

}
