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

import java.util.HashSet;
import java.util.Set;

import edu.uci.ics.hyracks.algebricks.common.utils.Pair;

/**
 * Collection of hints supported by create dataset statement.
 * Includes method to validate a hint and its associated value
 * as provided in a create dataset statement.
 */
public class DatasetHints {

    /**
     * validate the use of a hint
     * 
     * @param hintName
     *            name of the hint
     * @param value
     *            value associated with the hint
     * @return a Pair with
     *         first element as a boolean that represents the validation result.
     *         second element as the error message if the validation result is false
     */
    public static Pair<Boolean, String> validate(String hintName, String value) {
        for (IHint h : hints) {
            if (h.getName().equalsIgnoreCase(hintName.trim())) {
                return h.validateValue(value);
            }
        }
        return new Pair<Boolean, String>(false, "Unknwon hint :" + hintName);
    }

    private static Set<IHint> hints = initHints();

    private static Set<IHint> initHints() {
        Set<IHint> hints = new HashSet<IHint>();
        hints.add(new DatasetCardinalityHint());
        return hints;
    }

    /**
     * Hint representing the expected number of tuples in the dataset.
     */
    public static class DatasetCardinalityHint implements IHint {
        public static final String NAME = "CARDINALITY";

        public static final long DEFAULT = 1000000L;

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public Pair<Boolean, String> validateValue(String value) {
            boolean valid = true;
            long longValue;
            try {
                longValue = Long.parseLong(value);
                if (longValue < 0) {
                    return new Pair<Boolean, String>(false, "Value must be >= 0");
                }
            } catch (NumberFormatException nfe) {
                valid = false;
                return new Pair<Boolean, String>(valid, "Inappropriate value");
            }
            return new Pair<Boolean, String>(true, null);
        }

    }
}
