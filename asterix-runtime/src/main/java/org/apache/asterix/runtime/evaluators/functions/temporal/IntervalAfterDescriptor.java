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
package org.apache.asterix.runtime.evaluators.functions.temporal;

import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

public class IntervalAfterDescriptor extends AbstractIntervalLogicFuncDescriptor {

    private final static long serialVersionUID = 1L;
    public final static FunctionIdentifier FID = AsterixBuiltinFunctions.INTERVAL_AFTER;

    public final static IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {

        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new IntervalAfterDescriptor();
        }
    };

    /* (non-Javadoc)
     * @see org.apache.asterix.om.functions.IFunctionDescriptor#getIdentifier()
     */
    @Override
    public FunctionIdentifier getIdentifier() {
        return FID;
    }

    /* (non-Javadoc)
     * @see org.apache.asterix.runtime.evaluators.functions.temporal.AbstractIntervalLogicFuncDescriptor#compareIntervals(long, long, long, long)
     */
    @Override
    protected boolean compareIntervals(long s1, long e1, long s2, long e2) {
        return IntervalLogic.after(s1, e1, s2, e2);
    }

}
