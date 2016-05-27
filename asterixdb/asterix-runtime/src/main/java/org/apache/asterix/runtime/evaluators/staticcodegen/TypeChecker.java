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
package org.apache.asterix.runtime.evaluators.staticcodegen;

import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.data.std.api.IPointable;

/**
 * The missing/null-handling code to be injected into the evaluator(...) method of scalar evaluators.
 */
public class TypeChecker {

    private byte[] MISSING_BYTES = new byte[] { ATypeTag.SERIALIZED_MISSING_TYPE_TAG };
    private byte[] NULL_BYTES = new byte[] { ATypeTag.SERIALIZED_NULL_TYPE_TAG };
    private boolean meetNull = false;

    /**
     * This method should be called for each argument of a function.
     *
     * @param arg,
     *            the argument pointable.
     * @param resultPointable,
     *            the returned result for the function.
     * @return true if arg is MISSING; false otherwise.
     */
    public boolean isMissing(IPointable arg, IPointable resultPointable) {
        byte[] data = arg.getByteArray();
        int start = arg.getStartOffset();
        byte serializedTypeTag = data[start];
        if (serializedTypeTag == ATypeTag.SERIALIZED_MISSING_TYPE_TAG) {
            resultPointable.set(MISSING_BYTES, 0, 1);
            // resets meetNull for the next evaluate(...) call.
            meetNull = false;
            return true;
        }
        if (serializedTypeTag == ATypeTag.SERIALIZED_NULL_TYPE_TAG) {
            meetNull |= true;
        }
        return false;
    }

    /**
     * This method should be called after all arguments for a function are evaluated.
     *
     * @param resultPointable,
     *            the returned result for the function.
     * @return true if any argument is NULL; false otherwise.
     */
    public boolean isNull(IPointable resultPointable) {
        if (meetNull) {
            resultPointable.set(NULL_BYTES, 0, 1);
            // resets meetNull.
            meetNull = false;
            return true;
        }
        // resets meetNull for the next evaluate(...) call.
        meetNull = false;
        return false;
    }

}
