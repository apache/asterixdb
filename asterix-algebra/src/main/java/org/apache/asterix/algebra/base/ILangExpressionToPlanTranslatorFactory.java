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
package org.apache.asterix.algebra.base;

import org.apache.asterix.metadata.declared.AqlMetadataProvider;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;

public interface ILangExpressionToPlanTranslatorFactory {

    /**
     * @param metadataProvider,
     *            providing the definition of created (i.e., stored) user-defined functions.
     * @param currentVarCounter,
     *            the current minimum available variable id.
     * @return a logical query plan.
     * @throws AlgebricksException
     */
    ILangExpressionToPlanTranslator createExpressionToPlanTranslator(AqlMetadataProvider metadataProvider,
            int currentVarCountert) throws AlgebricksException;

}
