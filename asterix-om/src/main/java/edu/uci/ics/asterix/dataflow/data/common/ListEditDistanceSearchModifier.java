package edu.uci.ics.asterix.dataflow.data.common;

import java.util.Collections;
import java.util.List;

import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedIndexSearchModifier;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedListCursor;

// TODO: Should go into hyracks.
public class ListEditDistanceSearchModifier implements IInvertedIndexSearchModifier {

    private int edThresh;

    public ListEditDistanceSearchModifier(int edThresh) {
        this.edThresh = edThresh;
    }

    @Override
    public int getOccurrenceThreshold(List<IInvertedListCursor> invListCursors) {
        return invListCursors.size() - edThresh;
    }

    @Override
    public int getPrefixLists(List<IInvertedListCursor> invListCursors) {
        Collections.sort(invListCursors);
        return invListCursors.size() - getOccurrenceThreshold(invListCursors) + 1;
    }

    public int getEdThresh() {
        return edThresh;
    }

    public void setEdThresh(int edThresh) {
        this.edThresh = edThresh;
    }
}
