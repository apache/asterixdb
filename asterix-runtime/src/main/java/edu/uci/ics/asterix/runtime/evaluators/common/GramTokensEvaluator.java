package edu.uci.ics.asterix.runtime.evaluators.common;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.builders.IAOrderedListBuilder;
import edu.uci.ics.asterix.builders.OrderedListBuilder;
import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IDataOutputProvider;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.BooleanSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.IBinaryTokenizer;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.IToken;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.IntArray;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.NGramUTF8StringBinaryTokenizer;

public class GramTokensEvaluator implements ICopyEvaluator {

    // assuming type indicator in serde format
    private final int typeIndicatorSize = 1;

    protected final DataOutput out;
    protected final ArrayBackedValueStorage argOut = new ArrayBackedValueStorage();
    protected final ICopyEvaluator stringEval;
    protected final ICopyEvaluator gramLengthEval;
    protected final ICopyEvaluator prePostEval;

    private final NGramUTF8StringBinaryTokenizer tokenizer;

    protected final IntArray itemOffsets = new IntArray();
    protected final ArrayBackedValueStorage tokenBuffer = new ArrayBackedValueStorage();

    private IAOrderedListBuilder listBuilder = new OrderedListBuilder();
    private ArrayBackedValueStorage inputVal = new ArrayBackedValueStorage();
    private BuiltinType itemType;

    public GramTokensEvaluator(ICopyEvaluatorFactory[] args, IDataOutputProvider output, IBinaryTokenizer tokenizer,
            BuiltinType itemType) throws AlgebricksException {
        out = output.getDataOutput();
        stringEval = args[0].createEvaluator(argOut);
        gramLengthEval = args[1].createEvaluator(argOut);
        prePostEval = args[2].createEvaluator(argOut);
        this.tokenizer = (NGramUTF8StringBinaryTokenizer) tokenizer;
        this.itemType = itemType;
    }

    @Override
    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
        argOut.reset();
        stringEval.evaluate(tuple);
        int gramLengthOff = argOut.getLength();
        gramLengthEval.evaluate(tuple);
        int prePostOff = argOut.getLength();
        prePostEval.evaluate(tuple);

        byte[] bytes = argOut.getByteArray();
        int gramLength = IntegerSerializerDeserializer.getInt(bytes, gramLengthOff + typeIndicatorSize);
        tokenizer.setGramlength(gramLength);
        boolean prePost = BooleanSerializerDeserializer.getBoolean(bytes, prePostOff + typeIndicatorSize);
        tokenizer.setPrePost(prePost);
        tokenizer.reset(bytes, 0, gramLengthOff);
        tokenBuffer.reset();

        try {
            listBuilder.reset(new AOrderedListType(itemType, null));
            while (tokenizer.hasNext()) {
                inputVal.reset();
                tokenizer.next();
                IToken token = tokenizer.getToken();
                token.serializeToken(inputVal.getDataOutput());
                listBuilder.addItem(inputVal);
            }
            listBuilder.write(out, true);
        } catch (IOException e) {
            throw new AlgebricksException(e);
        }
    }
}
