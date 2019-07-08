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
package org.apache.asterix.runtime.evaluators.common;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizer;

public class WordTokensEvaluator implements IScalarEvaluator {
    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    private final DataOutput out = resultStorage.getDataOutput();
    private final IPointable argPtr = new VoidPointable();
    private final IScalarEvaluator stringEval;

    private final IBinaryTokenizer tokenizer;
    private final OrderedListBuilder listBuilder = new OrderedListBuilder();
    private final AOrderedListType listType;

    public WordTokensEvaluator(IScalarEvaluatorFactory[] args, IEvaluatorContext context, IBinaryTokenizer tokenizer,
            BuiltinType itemType) throws HyracksDataException {
        stringEval = args[0].createScalarEvaluator(context);
        this.tokenizer = tokenizer;
        this.listType = new AOrderedListType(itemType, null);
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        resultStorage.reset();
        stringEval.evaluate(tuple, argPtr);

        if (PointableHelper.checkAndSetMissingOrNull(result, argPtr)) {
            return;
        }

        tokenizer.reset(argPtr.getByteArray(), argPtr.getStartOffset(), argPtr.getLength());
        try {
            listBuilder.reset(listType);
            while (tokenizer.hasNext()) {
                tokenizer.next();
                listBuilder.addItem(tokenizer.getToken());
            }
            listBuilder.write(out, true);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
        result.set(resultStorage);
    }
}
