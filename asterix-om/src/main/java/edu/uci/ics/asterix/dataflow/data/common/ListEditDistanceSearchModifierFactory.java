package edu.uci.ics.asterix.dataflow.data.common;

import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedIndexSearchModifier;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedIndexSearchModifierFactory;

// TODO: Should go into hyracks.
public class ListEditDistanceSearchModifierFactory implements IInvertedIndexSearchModifierFactory {

    private static final long serialVersionUID = 1L;

    private final int edThresh;
    
    public ListEditDistanceSearchModifierFactory(int edThresh) {
        this.edThresh = edThresh;
    }
    
    @Override
    public IInvertedIndexSearchModifier createSearchModifier() {
        return new ListEditDistanceSearchModifier(edThresh);
    }
}
