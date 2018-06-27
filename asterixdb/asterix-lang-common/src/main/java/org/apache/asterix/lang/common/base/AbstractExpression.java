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
package org.apache.asterix.lang.common.base;

import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionAnnotation;

/**
 * All subclasses need to make sure the hints are copied over in the DeepCopyVisitor and
 * CloneAndSubstituteVariablesVisitor
 */
public abstract class AbstractExpression extends AbstractLangExpression implements Expression {

    protected List<IExpressionAnnotation> hints;

    public void addHint(IExpressionAnnotation hint) {
        if (hint == null) {
            return;
        }
        if (hints == null) {
            hints = new ArrayList<>();
        }
        hints.add(hint);
    }

    public void addHints(List<IExpressionAnnotation> newHints) {
        if (newHints == null) {
            return;
        }
        if (hints == null) {
            hints = new ArrayList<>();
        }
        hints.addAll(newHints);
    }

    public boolean hasHints() {
        return hints != null;
    }

    public List<IExpressionAnnotation> getHints() {
        return hints;
    }
}
