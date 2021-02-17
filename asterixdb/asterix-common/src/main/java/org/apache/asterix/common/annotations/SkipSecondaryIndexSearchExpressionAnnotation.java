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
package org.apache.asterix.common.annotations;

import java.util.Collection;

public final class SkipSecondaryIndexSearchExpressionAnnotation extends AbstractExpressionAnnotationWithIndexNames {

    public static final String HINT_STRING = "skip-index";

    public static final SkipSecondaryIndexSearchExpressionAnnotation INSTANCE_ANY_INDEX =
            new SkipSecondaryIndexSearchExpressionAnnotation(null);

    private SkipSecondaryIndexSearchExpressionAnnotation(Collection<String> indexNames) {
        super(indexNames);
    }

    public static SkipSecondaryIndexSearchExpressionAnnotation newInstance(Collection<String> indexNames) {
        if (indexNames == null || indexNames.isEmpty()) {
            throw new IllegalArgumentException();
        }
        return new SkipSecondaryIndexSearchExpressionAnnotation(indexNames);
    }

    @Override
    public String toString() {
        return indexNames == null ? HINT_STRING : HINT_STRING + indexNames;
    }
}
