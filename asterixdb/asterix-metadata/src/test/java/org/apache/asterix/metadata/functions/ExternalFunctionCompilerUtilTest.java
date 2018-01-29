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

import org.apache.asterix.common.exceptions.MetadataException;
import org.apache.asterix.common.transactions.JobId;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.entities.Function;
import org.apache.asterix.om.types.AUnorderedListType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedList;

public class ExternalFunctionCompilerUtilTest {
    @Test
    public void test() throws MetadataException, AlgebricksException {
            // given
        MetadataTransactionContext txnCtx = new MetadataTransactionContext(new JobId(1));
        Function function = new Function("test", "test", 0, new LinkedList<String>(), "{{ASTRING}}", "", "JAVA", "SCALAR");

            // when
        ExternalScalarFunctionInfo info = (ExternalScalarFunctionInfo) ExternalFunctionCompilerUtil.getExternalFunctionInfo(txnCtx, function);
        IAType type = info.getResultTypeComputer().computeType(null, null, null);

            // then
        IAType expectedType = new AUnorderedListType(BuiltinType.ASTRING, "AUnorderedList");
        Assert.assertEquals(expectedType, info.getReturnType());
        Assert.assertEquals(expectedType, type);
    }
}
