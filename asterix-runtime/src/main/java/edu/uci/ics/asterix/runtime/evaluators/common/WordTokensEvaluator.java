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
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.IBinaryTokenizer;

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
