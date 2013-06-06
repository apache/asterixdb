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
package edu.uci.ics.asterix.runtime.evaluators.common;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.builders.OrderedListBuilder;
import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizer;

public class WordTokensEvaluator implements ICopyEvaluator {
    private final DataOutput out;
    private final ArrayBackedValueStorage argOut = new ArrayBackedValueStorage();
    private final ICopyEvaluator stringEval;

    private final IBinaryTokenizer tokenizer;
    private final OrderedListBuilder listBuilder = new OrderedListBuilder();
    private final AOrderedListType listType;

    public WordTokensEvaluator(ICopyEvaluatorFactory[] args, IDataOutputProvider output, IBinaryTokenizer tokenizer,
            BuiltinType itemType) throws AlgebricksException {
        out = output.getDataOutput();
        stringEval = args[0].createEvaluator(argOut);
        this.tokenizer = tokenizer;
        this.listType = new AOrderedListType(itemType, null);
    }

    @Override
    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
        argOut.reset();
        stringEval.evaluate(tuple);
        byte[] bytes = argOut.getByteArray();
        tokenizer.reset(bytes, 0, argOut.getLength());
        try {
            listBuilder.reset(listType);
            while (tokenizer.hasNext()) {
                tokenizer.next();
                listBuilder.addItem(tokenizer.getToken());
            }
            listBuilder.write(out, true);
        } catch (IOException e) {
            throw new AlgebricksException(e);
        }
    }
}
