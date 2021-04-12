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
 */
package org.apache.asterix.common.exceptions;

import java.util.Collection;

import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.Warning;

public class WarningUtil {

    private WarningUtil() {
    }

    /**
     * Merges the warnings from the collection argument into the warning collector argument.
     */
    public static void mergeWarnings(Collection<Warning> warnings, IWarningCollector warningsCollector) {
        for (Warning warning : warnings) {
            if (warningsCollector.shouldWarn()) {
                warningsCollector.warn(warning);
            }
        }
    }
}
