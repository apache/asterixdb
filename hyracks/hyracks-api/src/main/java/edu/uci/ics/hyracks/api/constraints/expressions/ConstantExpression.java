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
package edu.uci.ics.hyracks.api.constraints.expressions;

import java.util.Collection;

public class ConstantExpression extends ConstraintExpression {
    private static final long serialVersionUID = 1L;

    private final Object value;

    public ConstantExpression(Object value) {
        this.value = value;
    }

    @Override
    public ExpressionTag getTag() {
        return ExpressionTag.CONSTANT;
    }

    public Object getValue() {
        return value;
    }

    @Override
    public void getChildren(Collection<ConstraintExpression> children) {
    }

    @Override
    protected void toString(StringBuilder buffer) {
        buffer.append(getTag()).append('[').append(value).append(':').append(value.getClass().getName()).append(']');
    }
}