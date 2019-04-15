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
package org.apache.asterix.metadata.functions;

import java.util.LinkedList;

import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.transactions.TxnId;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.entities.Function;
import org.apache.asterix.om.types.AUnorderedListType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.junit.Assert;
import org.junit.Test;

public class ExternalFunctionCompilerUtilTest {
    @Test
    public void test() throws AlgebricksException {
        // given
        MetadataTransactionContext txnCtx = new MetadataTransactionContext(new TxnId(1));
        FunctionSignature signature = new FunctionSignature("test", "test", 0);
        Function function = new Function(signature, new LinkedList<>(), "{{ASTRING}}", "", "JAVA", "SCALAR", null);

        // when
        ExternalScalarFunctionInfo info =
                (ExternalScalarFunctionInfo) ExternalFunctionCompilerUtil.getExternalFunctionInfo(txnCtx, function);

        // then
        IAType expectedType = new AUnorderedListType(BuiltinType.ASTRING, "AUnorderedList");
        Assert.assertEquals(expectedType, info.getReturnType());
    }
}
