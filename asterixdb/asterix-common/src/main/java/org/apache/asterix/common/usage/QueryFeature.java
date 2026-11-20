/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */
package org.apache.asterix.common.usage;

import java.util.Locale;

/**
 * The query features whose use is recorded for each statement. Compilation marks the ones a statement uses on its
 * {@link StatementUsage}; the list is closed, so that anything keyed on it stays bounded.
 */
public enum QueryFeature {
    ANALYZE,
    ADVISE,
    CLUSTER_BY,
    COPY_FROM,
    COPY_TO,
    /** a call to a vector distance or similarity function, with or without a vector index to answer it */
    VECTOR_SEARCH,
    /** a call to a Python function, counted once however many calls the statement makes */
    PYTHON_UDF;

    private final String label = name().toLowerCase(Locale.ROOT);

    /** @return the feature's name in lower case, such as {@code cluster_by} */
    public String label() {
        return label;
    }
}
