/*
 * Copyright 2009-2013 by The Regents of the University of California
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
package edu.uci.ics.asterix.metadata.dataset.hints;

import edu.uci.ics.hyracks.algebricks.common.utils.Pair;

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
    public Pair<Boolean, String> validateValue(String value);

}
