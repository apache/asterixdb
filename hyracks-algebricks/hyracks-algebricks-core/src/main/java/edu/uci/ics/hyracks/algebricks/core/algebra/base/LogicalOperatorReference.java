/*
 * Copyright 2009-2010 by The Regents of the University of California
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
package edu.uci.ics.hyracks.algebricks.core.algebra.base;

public final class LogicalOperatorReference {
    private ILogicalOperator operator;

    public LogicalOperatorReference() {
    }

    public LogicalOperatorReference(ILogicalOperator operator) {
        this.operator = operator;
    }

    public ILogicalOperator getOperator() {
        return operator;
    }

    public void setOperator(ILogicalOperator operator) {
        this.operator = operator;
    }

    @Override
    public String toString() {
        return operator == null ? "" : operator.toString();
    }
}
