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

import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;

public final class FunctionalDependency {
    private List<LogicalVariable> head;
    private List<LogicalVariable> tail;

    public FunctionalDependency(List<LogicalVariable> head, List<LogicalVariable> tail) {
        this.head = head;
        this.tail = tail;
    }

    public List<LogicalVariable> getHead() {
        return head;
    }

    public List<LogicalVariable> getTail() {
        return tail;
    }

    @Override
    public String toString() {
        return head + "->" + tail;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof FunctionalDependency)) {
            return false;
        } else {
            FunctionalDependency fd = (FunctionalDependency) obj;
            return fd.getHead().equals(this.head) && fd.getTail().equals(this.tail);
        }
    }

}
