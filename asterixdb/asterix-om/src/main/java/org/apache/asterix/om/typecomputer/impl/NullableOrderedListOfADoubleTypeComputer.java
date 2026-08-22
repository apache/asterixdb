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
package org.apache.asterix.om.typecomputer.impl;

import org.apache.asterix.om.typecomputer.base.IResultTypeComputer;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

/**
 * An ordered list of doubles that may be NULL — the final type of the CENTROID aggregate.
 * {@code AbstractCentroidAggregateFunction#finishFinalResults} writes NULL for an empty cluster and for a
 * NULL aggregate type, so a plain {@link OrderedListOfADoubleTypeComputer} would declare a type the function
 * does not always produce. AVG declares its final type nullable for the same reason.
 */
public class NullableOrderedListOfADoubleTypeComputer implements IResultTypeComputer {

    public static final NullableOrderedListOfADoubleTypeComputer INSTANCE =
            new NullableOrderedListOfADoubleTypeComputer();

    private NullableOrderedListOfADoubleTypeComputer() {
    }

    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {
        return AUnionType.createNullableType(new AOrderedListType(BuiltinType.ADOUBLE, null), "OptionalDoubleList");
    }
}
