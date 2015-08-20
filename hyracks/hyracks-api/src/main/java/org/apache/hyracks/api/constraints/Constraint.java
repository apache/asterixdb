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
package edu.uci.ics.hyracks.api.constraints;

import java.io.Serializable;

import edu.uci.ics.hyracks.api.constraints.expressions.ConstraintExpression;
import edu.uci.ics.hyracks.api.constraints.expressions.LValueConstraintExpression;

public class Constraint implements Serializable {
    private static final long serialVersionUID = 1L;

    private final LValueConstraintExpression lValue;

    private final ConstraintExpression rValue;

    public Constraint(LValueConstraintExpression lValue, ConstraintExpression rValue) {
        this.lValue = lValue;
        this.rValue = rValue;
    }

    public LValueConstraintExpression getLValue() {
        return lValue;
    }

    public ConstraintExpression getRValue() {
        return rValue;
    }

    @Override
    public String toString() {
        return lValue + " in " + rValue;
    }
}