package edu.uci.ics.asterix.builders;

import java.io.DataOutput;

import edu.uci.ics.asterix.om.types.AbstractCollectionType;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.api.IValueReference;

public interface IAsterixListBuilder {
    /**
     * @param listType
     *            Type of the list: AUnorderedListType or AOrderedListType.
     */
    public void reset(AbstractCollectionType listType) throws HyracksDataException;

    /**
     * @param item
     *            Item to be added to the list.
     */
    public void addItem(IValueReference item) throws HyracksDataException;

    /**
     * @param out
     *            Stream to serialize the list into.
     * @param writeTypeTag
     *            Whether to write the list's type tag before the list data.
     */
    public void write(DataOutput out, boolean writeTypeTag) throws HyracksDataException;
}
