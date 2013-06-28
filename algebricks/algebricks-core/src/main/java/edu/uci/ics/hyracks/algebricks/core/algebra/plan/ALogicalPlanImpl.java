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
package edu.uci.ics.hyracks.algebricks.core.algebra.plan;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlan;

/*
 * Author: Guangqiang Li
 * Created on Jul 9, 2009 
 */
public class ALogicalPlanImpl implements ILogicalPlan {
    private List<Mutable<ILogicalOperator>> roots;

    public ALogicalPlanImpl() {
        this.roots = new ArrayList<Mutable<ILogicalOperator>>();
    }

    public ALogicalPlanImpl(List<Mutable<ILogicalOperator>> roots) {
        this.roots = roots;
    }

    public ALogicalPlanImpl(Mutable<ILogicalOperator> root) {
        roots = new ArrayList<Mutable<ILogicalOperator>>(1);
        roots.add(root);
    }

    public List<Mutable<ILogicalOperator>> getRoots() {
        return roots;
    }

    public void setRoots(List<Mutable<ILogicalOperator>> roots) {
        this.roots = roots;
    }
}
