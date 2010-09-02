package edu.uci.ics.hyracks.storage.am.btree.dataflow;

public class BTreeRegistryProvider implements IBTreeRegistryProvider {
		
	private static final long serialVersionUID = 1L;
	
	private static BTreeRegistry btreeRegistry = null;
	
	@Override
	public BTreeRegistry getBTreeRegistry() {
		if(btreeRegistry == null) {
			btreeRegistry = new BTreeRegistry();
		}		
		return btreeRegistry;
	}	
}
