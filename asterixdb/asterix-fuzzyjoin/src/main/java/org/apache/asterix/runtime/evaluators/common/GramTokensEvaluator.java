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
import org.apache.asterix.dataflow.data.nontagged.serde.ABooleanSerializerDeserializer;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
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
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.NGramUTF8StringBinaryTokenizer;

public class GramTokensEvaluator implements IScalarEvaluator {

    // assuming type indicator in serde format
    private final int typeIndicatorSize = 1;

    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    private final DataOutput out = resultStorage.getDataOutput();
    private final IPointable stringArg = new VoidPointable();
    private final IPointable gramLengthArg = new VoidPointable();
    private final IPointable prePostArg = new VoidPointable();
    private final IScalarEvaluator stringEval;
    private final IScalarEvaluator gramLengthEval;
    private final IScalarEvaluator prePostEval;

    private final NGramUTF8StringBinaryTokenizer tokenizer;
    private final OrderedListBuilder listBuilder = new OrderedListBuilder();
    private final AOrderedListType listType;

    public GramTokensEvaluator(IScalarEvaluatorFactory[] args, IEvaluatorContext context, IBinaryTokenizer tokenizer,
            BuiltinType itemType) throws HyracksDataException {
        stringEval = args[0].createScalarEvaluator(context);
        gramLengthEval = args[1].createScalarEvaluator(context);
        prePostEval = args[2].createScalarEvaluator(context);
        this.tokenizer = (NGramUTF8StringBinaryTokenizer) tokenizer;
        this.listType = new AOrderedListType(itemType, null);
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        resultStorage.reset();
        stringEval.evaluate(tuple, stringArg);
        gramLengthEval.evaluate(tuple, gramLengthArg);
        prePostEval.evaluate(tuple, prePostArg);

        if (PointableHelper.checkAndSetMissingOrNull(result, stringArg, gramLengthArg, prePostArg)) {
            return;
        }

        int gramLength = ATypeHierarchy.getIntegerValue(BuiltinFunctions.GRAM_TOKENS.getName(), 1,
                gramLengthArg.getByteArray(), gramLengthArg.getStartOffset());
        tokenizer.setGramlength(gramLength);
        boolean prePost = ABooleanSerializerDeserializer.getBoolean(prePostArg.getByteArray(),
                prePostArg.getStartOffset() + typeIndicatorSize);
        tokenizer.setPrePost(prePost);
        tokenizer.reset(stringArg.getByteArray(), stringArg.getStartOffset(), stringArg.getLength());

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
