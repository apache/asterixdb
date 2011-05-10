package edu.uci.ics.hyracks.storage.am.invertedindex.searchmodifiers;

import java.util.Collections;
import java.util.List;

import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedListCursor;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedIndexSearchModifier;

public class ConjunctiveSearchModifier implements IInvertedIndexSearchModifier {

    @Override
    public int getOccurrenceThreshold(List<IInvertedListCursor> invListCursors) {
        return invListCursors.size();
    }
    
    @Override
    public int getPrefixLists(List<IInvertedListCursor> invListCursors) {
        Collections.sort(invListCursors);
        return 1;
    }

}
