package edu.uci.ics.hyracks.storage.am.invertedindex.searchmodifiers;

import java.util.Collections;
import java.util.List;

import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedListCursor;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedIndexSearchModifier;

public class JaccardSearchModifier implements IInvertedIndexSearchModifier {
    
    private float jaccThresh;
    
    public JaccardSearchModifier(float jaccThresh) {
        this.jaccThresh = jaccThresh;
    }
    
    @Override
    public int getOccurrenceThreshold(List<IInvertedListCursor> invListCursors) {
        return (int) Math.floor((float) invListCursors.size() * jaccThresh);
    }

    @Override
    public int getPrefixLists(List<IInvertedListCursor> invListCursors) {
        Collections.sort(invListCursors);
        if (invListCursors.size() == 0) {
            return 0;
        }
        return invListCursors.size() - (int) Math.ceil(jaccThresh * invListCursors.size()) + 1;
    }

    public float getJaccThresh() {
        return jaccThresh;
    }

    public void setJaccThresh(float jaccThresh) {
        this.jaccThresh = jaccThresh;
    }
}
