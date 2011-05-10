package edu.uci.ics.hyracks.storage.am.invertedindex.searchmodifiers;

import java.util.Collections;
import java.util.List;

import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedListCursor;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedIndexSearchModifier;

public class EditDistanceSearchModifier implements IInvertedIndexSearchModifier {

    private int gramLength;
    private int edThresh;
    
    public EditDistanceSearchModifier(int gramLength, int edThresh) {
        this.gramLength = gramLength;
        this.edThresh = edThresh;
    }
    
    @Override
    public int getOccurrenceThreshold(List<IInvertedListCursor> invListCursors) {        
        return invListCursors.size() - edThresh * gramLength;
    }

    @Override
    public int getPrefixLists(List<IInvertedListCursor> invListCursors) {
        Collections.sort(invListCursors);          
        return invListCursors.size() - getOccurrenceThreshold(invListCursors) + 1;
    }

    public int getGramLength() {
        return gramLength;
    }

    public void setGramLength(int gramLength) {
        this.gramLength = gramLength;
    }

    public int getEdThresh() {
        return edThresh;
    }

    public void setEdThresh(int edThresh) {
        this.edThresh = edThresh;
    }       
}
