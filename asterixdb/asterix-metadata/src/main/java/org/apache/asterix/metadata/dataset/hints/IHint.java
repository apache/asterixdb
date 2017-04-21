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
package org.apache.asterix.metadata.dataset.hints;

import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.hyracks.algebricks.common.utils.Pair;

/**
 * Represents a hint provided as part of an AQL statement.
 */
public interface IHint {

    /**
     * retrieve the name of the hint.
     *
     * @return
     */
    public String getName();

    /**
     * validate the value associated with the hint.
     *
     * @param value
     *            the value associated with the hint.
     * @return a Pair with
     *         first element as a boolean that represents the validation result.
     *         second element as the error message if the validation result is false
     */
    public Pair<Boolean, String> validateValue(ICcApplicationContext appCtx, String value);

}
