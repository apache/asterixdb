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
package org.apache.asterix.external.classad.object.pool;

import org.apache.asterix.external.classad.CaseInsensitiveString;
import org.apache.asterix.external.classad.ExprTree;

public class ClassAdObjectPool {
    public final ExprHolderPool mutableExprPool;
    public final TokenValuePool tokenValuePool;
    public final ClassAdPool classAdPool;
    public final ExprListPool exprListPool;
    public final ValuePool valuePool;
    public final LiteralPool literalPool;
    public final BitSetPool bitSetPool;
    public final OperationPool operationPool;
    public final AttributeReferencePool attrRefPool;
    public final ClassAdParserPool classAdParserPool;
    public final AMutableCharArrayStringPool strPool;
    public final FunctionCallPool funcPool;
    public final MutableBooleanPool boolPool;
    public final CaseInsensitiveStringPool caseInsensitiveStringPool;
    public final StringArrayListPool stringArrayListPool;
    public final Int32Pool int32Pool;
    public final Int64Pool int64Pool;
    public final ClassAdTimePool classAdTimePool;
    public final DoublePool doublePool;
    public final EvalStatePool evalStatePool;
    public final HashMapPool<CaseInsensitiveString, ExprTree> strToExprPool;
    public final PrettyPrintPool prettyPrintPool;
    public final TreeSetPool<String> strSetPool;
    public final MutableNumberFactorPool numFactorPool;

    public ClassAdObjectPool() {
        int32Pool = new Int32Pool();
        int64Pool = new Int64Pool();
        mutableExprPool = new ExprHolderPool(this);
        tokenValuePool = new TokenValuePool();
        classAdPool = new ClassAdPool(this);
        exprListPool = new ExprListPool(this);
        valuePool = new ValuePool(this);
        literalPool = new LiteralPool(this);
        bitSetPool = new BitSetPool();
        operationPool = new OperationPool(this);
        attrRefPool = new AttributeReferencePool(this);
        strPool = new AMutableCharArrayStringPool();
        funcPool = new FunctionCallPool(this);
        boolPool = new MutableBooleanPool();
        caseInsensitiveStringPool = new CaseInsensitiveStringPool();
        stringArrayListPool = new StringArrayListPool();
        classAdParserPool = new ClassAdParserPool(this);
        classAdTimePool = new ClassAdTimePool();
        doublePool = new DoublePool();
        evalStatePool = new EvalStatePool(this);
        strToExprPool = new HashMapPool<CaseInsensitiveString, ExprTree>();
        prettyPrintPool = new PrettyPrintPool(this);
        strSetPool = new TreeSetPool<String>();
        numFactorPool = new MutableNumberFactorPool();
    }

    public void reset() {
        mutableExprPool.reset();
        tokenValuePool.reset();
        classAdPool.reset();
        exprListPool.reset();
        valuePool.reset();
        literalPool.reset();
        bitSetPool.reset();
        operationPool.reset();
        attrRefPool.reset();
        strPool.reset();
        classAdParserPool.reset();
        funcPool.reset();
        boolPool.reset();
        caseInsensitiveStringPool.reset();
        stringArrayListPool.reset();
        int32Pool.reset();
        int64Pool.reset();
        classAdTimePool.reset();
        doublePool.reset();
        evalStatePool.reset();
        strToExprPool.reset();
        prettyPrintPool.reset();
        strSetPool.reset();
        numFactorPool.reset();
    }
}
