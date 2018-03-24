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
package org.apache.asterix.geo.evaluators;

import org.apache.asterix.common.config.CompilerProperties;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionTypeInferer;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.RecordUtil;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;

public class GeoFunctionTypeInferers {
    private GeoFunctionTypeInferers() {
    }

    public static final class GeometryConstructorTypeInferer implements IFunctionTypeInferer {
        @Override
        public void infer(ILogicalExpression expr, IFunctionDescriptor fd, IVariableTypeEnvironment context,
                CompilerProperties compilerProps) throws AlgebricksException {
            AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expr;
            IAType t = (IAType) context.getType(fce.getArguments().get(0).getValue());
            ATypeTag typeTag = t.getTypeTag();
            if (typeTag.equals(ATypeTag.OBJECT)) {
                fd.setImmutableStates(t);
            } else if (typeTag.equals(ATypeTag.ANY)) {
                fd.setImmutableStates(RecordUtil.FULLY_OPEN_RECORD_TYPE);
            } else {
                throw new NotImplementedException("parse-geojson for data of type " + t);
            }
        }
    }

}
