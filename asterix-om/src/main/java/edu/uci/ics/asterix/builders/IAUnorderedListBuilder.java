package edu.uci.ics.asterix.builders;

import java.io.DataOutput;

import edu.uci.ics.asterix.om.types.AUnorderedListType;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.api.IValueReference;

public interface IAUnorderedListBuilder {

    /**
     * @param unorderedlistType
     *            - the type of the list.
     */
    public void reset(AUnorderedListType unorderedlistType) throws HyracksDataException;

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
