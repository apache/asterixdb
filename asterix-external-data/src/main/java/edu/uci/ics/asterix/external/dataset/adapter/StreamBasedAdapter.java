package edu.uci.ics.asterix.external.dataset.adapter;

import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.metadata.feeds.AdapterRuntimeManager;
import edu.uci.ics.asterix.metadata.feeds.IDatasourceAdapter;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParser;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;

public abstract class StreamBasedAdapter implements IDatasourceAdapter {

    private static final long serialVersionUID = 1L;

    protected static final Logger LOGGER = Logger.getLogger(StreamBasedAdapter.class.getName());

    public static final String NODE_RESOLVER_FACTORY_PROPERTY = "node.Resolver";

    public abstract InputStream getInputStream(int partition) throws IOException;

    protected final ITupleParser tupleParser;
    protected final IAType sourceDatatype;
    protected IHyracksTaskContext ctx;
    protected AdapterRuntimeManager runtimeManager;

    public StreamBasedAdapter(ITupleParserFactory parserFactory, IAType sourceDatatype, IHyracksTaskContext ctx)
            throws HyracksDataException {
        this.tupleParser = parserFactory.createTupleParser(ctx);
        this.sourceDatatype = sourceDatatype;
    }

    @Override
    public void start(int partition, IFrameWriter writer) throws Exception {
        InputStream in = getInputStream(partition);
        if (in != null) {
            tupleParser.parse(in, writer);
        } else {
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Could not obtain input stream for parsing from adaptor " + this + "[" + partition + "]");
            }
        }
    }
}
