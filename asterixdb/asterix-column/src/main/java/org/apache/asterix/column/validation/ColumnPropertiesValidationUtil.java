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
package org.apache.asterix.column.validation;

import java.util.List;
import java.util.Set;

import org.apache.asterix.common.config.DatasetConfig;
import org.apache.asterix.common.context.CorrelatedPrefixMergePolicyFactory;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class ColumnPropertiesValidationUtil {
    private static final Set<String> UNSUPPORTED_MERGE_POLICIES = Set.of(CorrelatedPrefixMergePolicyFactory.NAME);

    private ColumnPropertiesValidationUtil() {
    }

    public static void validate(SourceLocation sourceLocation, DatasetConfig.DatasetFormat format, String mergePolicy,
            List<String> filterFields) throws AlgebricksException {
        if (format != DatasetConfig.DatasetFormat.COLUMN) {
            return;
        }

        if (UNSUPPORTED_MERGE_POLICIES.contains(mergePolicy.toLowerCase())) {
            throw CompilationException.create(ErrorCode.UNSUPPORTED_COLUMN_MERGE_POLICY, sourceLocation, mergePolicy);
        }

        if (filterFields != null && !filterFields.isEmpty()) {
            throw CompilationException.create(ErrorCode.UNSUPPORTED_COLUMN_LSM_FILTER, sourceLocation);
        }
    }

}
