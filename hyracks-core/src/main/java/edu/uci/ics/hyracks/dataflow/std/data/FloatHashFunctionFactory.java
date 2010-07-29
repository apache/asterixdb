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
package edu.uci.ics.hyracks.dataflow.std.data;

import edu.uci.ics.hyracks.api.dataflow.value.IHashFunction;
import edu.uci.ics.hyracks.api.dataflow.value.IHashFunctionFactory;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class FloatHashFunctionFactory implements IHashFunctionFactory<Float> {
    private static final long serialVersionUID = 1L;

    public static final FloatHashFunctionFactory INSTANCE = new FloatHashFunctionFactory();

    private FloatHashFunctionFactory() {
    }

    @Override
    public IHashFunction<Float> createHashFunction() {
        return new IHashFunction<Float>() {
            @Override
            public int hash(Float o) throws HyracksDataException {
                return o.hashCode();
            }
        };
    }
}