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
package org.apache.asterix.column.values.writer.filters;

/**
 * Since the Duration is not comparable, hence not using it for min-max stats.
 */
public class DurationColumnFilterWriter extends AbstractColumnFilterWriter {

    @Override
    public long getMinNormalizedValue() {
        return Long.MIN_VALUE;
    }

    @Override
    public long getMaxNormalizedValue() {
        return Long.MAX_VALUE;
    }

    @Override
    public void reset() {

    }
}
