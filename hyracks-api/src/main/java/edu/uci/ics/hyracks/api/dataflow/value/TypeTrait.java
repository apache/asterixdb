package edu.uci.ics.hyracks.api.dataflow.value;

public class TypeTrait implements ITypeTrait {
	
	private static final long serialVersionUID = 1L;
	private int length;
	
	public TypeTrait(int length) {
		this.length = length;
	}
	
	@Override
	public int getStaticallyKnownDataLength() {
		return length;
	}	
}
