package edu.uci.ics.pregelix.dataflow;

import java.nio.ByteBuffer;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import edu.uci.ics.pregelix.dataflow.base.IConfigurationFactory;
import edu.uci.ics.pregelix.dataflow.util.IterationUtils;

public class TerminationStateWriterOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(TerminationStateWriterOperatorDescriptor.class.getName());

    private final IConfigurationFactory confFactory;
    private final String jobId;

    public TerminationStateWriterOperatorDescriptor(JobSpecification spec, IConfigurationFactory confFactory,
            String jobId) {
        super(spec, 1, 0);
        this.confFactory = confFactory;
        this.jobId = jobId;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        return new AbstractUnaryInputSinkOperatorNodePushable() {
            private Configuration conf = confFactory.createConfiguration();
            private boolean terminate = true;

            @Override
            public void open() throws HyracksDataException {
                terminate = true;
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                terminate = false;
            }

            @Override
            public void fail() throws HyracksDataException {

            }

            @Override
            public void close() throws HyracksDataException {
                IterationUtils.writeTerminationState(conf, jobId, terminate);
                LOGGER.info("close termination state");
                if (terminate)
                    LOGGER.info("write termination to HDFS");
            }

        };
    }
}
