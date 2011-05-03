package edu.uci.ics.hyracks.storage.am.invertedindex.api;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;

public interface IInvertedListCursor extends Comparable<IInvertedListCursor> {
    void reset(int startPageId, int endPageId, int startOff, int numElements);
    
    void pinPagesSync() throws HyracksDataException;
    void pinPagesAsync() throws HyracksDataException;
    void unpinPages() throws HyracksDataException;
        
    boolean hasNext();
    void next();
            
    ITupleReference getTuple();
    
    // getters
    int getNumElements();
    int getStartPageId();
    int getEndPageId();
    int getStartOff();
    
    // jump to a specific element
    void positionCursor(int elementIx);
    boolean containsKey(ITupleReference searchTuple, MultiComparator invListCmp);      
            
    // for debugging
    String printInvList(ISerializerDeserializer[] serdes) throws HyracksDataException;    
    String printCurrentElement(ISerializerDeserializer[] serdes) throws HyracksDataException;  
}
