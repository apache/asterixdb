package edu.uci.ics.hyracks.storage.am.btree.api;

import java.io.Serializable;

public interface IBTreeTupleWriterFactory extends Serializable {
	public IBTreeTupleWriter createTupleWriter();
}
