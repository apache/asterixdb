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

package org.apache.asterix.common.exceptions;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.hyracks.util.annotations.NotThreadSafe;

/**
 * A warning collector that collects warnings up to {@link Long#MAX_VALUE} by default.
 */
@NotThreadSafe
public final class WarningCollector implements IWarningCollector {

    private final Set<Warning> warnings = new LinkedHashSet<>();
    private long maxWarnings = Long.MAX_VALUE;
    private long totalWarningsCount;

    public void clear() {
        warnings.clear();
        totalWarningsCount = 0;
    }

    @Override
    public void warn(Warning warning) {
        this.warnings.add(warning);
    }

    @Override
    public boolean shouldWarn() {
        return totalWarningsCount < Long.MAX_VALUE && totalWarningsCount++ < maxWarnings;
    }

    @Override
    public long getTotalWarningsCount() {
        return totalWarningsCount;
    }

    public void getWarnings(Collection<? super Warning> outWarnings, long maxWarnings) {
        long i = 0;
        for (Warning warning : warnings) {
            if (i >= maxWarnings) {
                break;
            }
            outWarnings.add(warning);
            i++;
        }
    }

    public void getWarnings(IWarningCollector outWarningCollector) {
        WarningUtil.mergeWarnings(warnings, outWarningCollector);
    }

    public void setMaxWarnings(long maxWarnings) {
        this.maxWarnings = maxWarnings;
    }
}
