package edu.uci.ics.asterix.runtime.evaluators.functions;

import edu.uci.ics.asterix.common.functions.FunctionConstants;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.common.WordTokensEvaluator;
import edu.uci.ics.fuzzyjoin.tokenizer.DelimitedUTF8StringBinaryTokenizer;
import edu.uci.ics.fuzzyjoin.tokenizer.IBinaryTokenizer;
import edu.uci.ics.fuzzyjoin.tokenizer.ITokenFactory;
import edu.uci.ics.fuzzyjoin.tokenizer.UTF8WordTokenFactory;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.IEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IEvaluatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IDataOutputProvider;

public class WordTokensDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    private final static FunctionIdentifier FID = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "word-tokens",
            1, true);
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new WordTokensDescriptor();
        }
    };

    @Override
    public FunctionIdentifier getIdentifier() {
        return FID;
    }

    @Override
    public IEvaluatorFactory createEvaluatorFactory(final IEvaluatorFactory[] args) throws AlgebricksException {
        return new IEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IEvaluator createEvaluator(IDataOutputProvider output) throws AlgebricksException {
                ITokenFactory tokenFactory = new UTF8WordTokenFactory(ATypeTag.STRING.serialize(),
                        ATypeTag.INT32.serialize());
                IBinaryTokenizer tokenizer = new DelimitedUTF8StringBinaryTokenizer(true, true, tokenFactory);
                return new WordTokensEvaluator(args, output, tokenizer, BuiltinType.ASTRING);
            }
        };
    }

}
