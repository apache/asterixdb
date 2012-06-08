package edu.uci.ics.asterix.builders;

import java.io.DataOutput;

import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.api.IValueReference;

public interface IAOrderedListBuilder {

    /**
     * @param orderedlistType
     *            - the type of the list.
     */
    public void reset(AOrderedListType orderedlistType) throws HyracksDataException;

    /**
     * @param item
     *            - the item to be added to the list.
     */
    public void addItem(IValueReference item) throws HyracksDataException;

    /**
     * @param out
     *            - Stream to write data to.
     */
    public void write(DataOutput out, boolean writeTypeTag) throws HyracksDataException;
}
