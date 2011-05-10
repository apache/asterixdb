package edu.uci.ics.hyracks.storage.am.invertedindex.api;

import java.util.List;

public interface IInvertedIndexSearchModifier {
    int getOccurrenceThreshold(List<IInvertedListCursor> invListCursors);
    int getPrefixLists(List<IInvertedListCursor> invListCursors);    
}
