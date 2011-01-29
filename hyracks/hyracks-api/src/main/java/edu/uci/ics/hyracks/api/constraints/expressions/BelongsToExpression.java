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
package edu.uci.ics.hyracks.api.constraints.expressions;

import java.util.Collection;

public final class BelongsToExpression extends ConstraintExpression {
    private static final long serialVersionUID = 1L;

    private final ConstraintExpression itemExpression;

    private final ConstraintExpression setExpression;

    public BelongsToExpression(ConstraintExpression itemExpression, ConstraintExpression setExpression) {
        this.itemExpression = itemExpression;
        this.setExpression = setExpression;
    }

    @Override
    public ExpressionTag getTag() {
        return ExpressionTag.BELONGS_TO;
    }

    public ConstraintExpression getItemExpression() {
        return itemExpression;
    }

    public ConstraintExpression getSetExpression() {
        return setExpression;
    }

    @Override
    public void getChildren(Collection<ConstraintExpression> children) {
        children.add(itemExpression);
        children.add(setExpression);
    }

    @Override
    protected void toString(StringBuilder buffer) {
        buffer.append(getTag()).append('(');
        itemExpression.toString(buffer);
        buffer.append(", ");
        setExpression.toString(buffer);
        buffer.append(')');
    }
}