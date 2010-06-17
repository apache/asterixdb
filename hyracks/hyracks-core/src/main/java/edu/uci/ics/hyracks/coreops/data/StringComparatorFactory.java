/*
 * Copyright 2009-2010 by The Regents of the University of California
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
package edu.uci.ics.hyracks.coreops.data;

import edu.uci.ics.hyracks.api.dataflow.value.IComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IComparatorFactory;

public class StringComparatorFactory implements IComparatorFactory<String> {
    private static final long serialVersionUID = 1L;

    public static final StringComparatorFactory INSTANCE = new StringComparatorFactory();

    private StringComparatorFactory() {
    }

    @Override
    public IComparator<String> createComparator() {
        return new IComparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                return o1.compareTo(o2);
            }
        };
    }
}