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
package edu.uci.ics.asterix.runtime.evaluators.functions.records;

import java.util.List;

import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;

public class FieldAccessNestedDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new FieldAccessNestedDescriptor();
        }
    };

    private ARecordType recType;
    private List<String> fldName;

    public void reset(ARecordType recType, List<String> fldName) {
        this.recType = recType;
        this.fldName = fldName;
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.FIELD_ACCESS_NESTED;
    }

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(ICopyEvaluatorFactory[] args) {
        return new FieldAccessNestedEvalFactory(args[0], recType, fldName);
    }

}