package edu.uci.ics.hyracks.dataflow.common.data.hash;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunction;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionGeneratorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.util.StringUtils;

public class UTF8StringBinaryHashFunctionGeneratorFactory implements IBinaryHashFunctionGeneratorFactory {
	
	
	public static final UTF8StringBinaryHashFunctionGeneratorFactory INSTANCE = new UTF8StringBinaryHashFunctionGeneratorFactory();
	private static final long serialVersionUID = 1L;
	
	static final int[] primeCoefficents = {31, 23, 53, 97, 71, 337, 11, 877, 3, 29};
	
	private UTF8StringBinaryHashFunctionGeneratorFactory(){
	}
	
	@Override
	public IBinaryHashFunction createBinaryHashFunction(int seed) {
		final int coefficient = primeCoefficents[seed % primeCoefficents.length];
		final int r = primeCoefficents[(seed+1) % primeCoefficents.length];
		
		return new IBinaryHashFunction() {
            @Override
            public int hash(byte[] bytes, int offset, int length) {
                int h = 0;
                int utflen = StringUtils.getUTFLen(bytes, offset);
                int sStart = offset + 2;
                int c = 0;

                while (c < utflen) {
                    char ch = StringUtils.charAt(bytes, sStart + c);
                    h = (coefficient * h + ch)%r;
                    c += StringUtils.charSize(bytes, sStart + c);
                }
                return h;
            }
        };
	}

}
