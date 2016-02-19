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
package org.apache.hyracks.api.constraints.expressions;

import java.io.Serializable;
import java.util.Collection;

public abstract class ConstraintExpression implements Serializable {
    private static final long serialVersionUID = 1L;

    public enum ExpressionTag {
        CONSTANT,
        PARTITION_COUNT,
        PARTITION_LOCATION,
    }

    public abstract ExpressionTag getTag();

    public abstract void getChildren(Collection<ConstraintExpression> children);

    @Override
    public final String toString() {
        StringBuilder buffer = new StringBuilder();
        toString(buffer);
        return buffer.toString();
    }

    protected abstract void toString(StringBuilder buffer);
}
