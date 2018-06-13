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
package org.apache.asterix.translator;

import java.util.Map;

import org.apache.asterix.algebra.base.ILangExpressionToPlanTranslator;
import org.apache.asterix.algebra.base.ILangExpressionToPlanTranslatorFactory;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.om.base.IAObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;

public class AqlExpressionToPlanTranslatorFactory implements ILangExpressionToPlanTranslatorFactory {

    @Override
    public ILangExpressionToPlanTranslator createExpressionToPlanTranslator(MetadataProvider metadataProvider,
            int currentVarCounter, Map<VarIdentifier, IAObject> externalVars) throws AlgebricksException {
        return new AqlExpressionToPlanTranslator(metadataProvider, currentVarCounter);
    }

}
