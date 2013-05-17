package edu.uci.ics.asterix.metadata.declared;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import edu.uci.ics.asterix.external.data.operator.FeedIntakeOperatorDescriptor;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.MetadataTransactionContext;
import edu.uci.ics.asterix.metadata.entities.Dataset;
import edu.uci.ics.asterix.metadata.entities.FeedDatasetDetails;
import edu.uci.ics.asterix.metadata.entities.FeedDatasetDetails.FeedState;
import edu.uci.ics.hyracks.algebricks.runtime.operators.meta.AlgebricksMetaOperatorDescriptor;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.context.IHyracksJobletContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.OperatorDescriptorId;
import edu.uci.ics.hyracks.api.job.IJobletEventListener;
import edu.uci.ics.hyracks.api.job.IJobletEventListenerFactory;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobInfo;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.api.job.JobStatus;

public class FeedJobEventListenerFactory implements IJobletEventListenerFactory {

    /**
	 * 
	 */
    private static final long serialVersionUID = 1L;

    private final Dataset dataset;
    private final JobSpecification jobSpec;
    private final IHyracksClientConnection hcc;
    private IJobletEventListener jobEventListener;

    public FeedJobEventListenerFactory(Dataset dataset, JobSpecification jobSpec, IHyracksClientConnection hcc) {
        this.dataset = dataset;
        this.jobSpec = jobSpec;
        this.hcc = hcc;
    }

    @Override
    public IJobletEventListener createListener(IHyracksJobletContext ctx) {

        jobEventListener = new IJobletEventListener() {

            private final List<String> ingestLocations = new ArrayList<String>();
            private final List<String> computeLocations = new ArrayList<String>();

            @Override
            public void jobletStart(JobId jobId) {
                List<OperatorDescriptorId> ingestOperatorIds = new ArrayList<OperatorDescriptorId>();
                List<OperatorDescriptorId> computeOperatorIds = new ArrayList<OperatorDescriptorId>();

                Map<OperatorDescriptorId, IOperatorDescriptor> operators = jobSpec.getOperatorMap();
                for (Entry<OperatorDescriptorId, IOperatorDescriptor> entry : operators.entrySet()) {
                    if (entry.getValue() instanceof FeedIntakeOperatorDescriptor) {
                        ingestOperatorIds.add(entry.getKey());
                    } else if (entry.getValue() instanceof AlgebricksMetaOperatorDescriptor) {
                        computeOperatorIds.add(entry.getKey());
                    }
                }

                try {
                    JobInfo info = hcc.getJobInfo(jobId);

                    for (OperatorDescriptorId ingestOpId : ingestOperatorIds) {
                        ingestLocations.addAll(info.getOperatorLocations().get(ingestOpId));
                    }
                    for (OperatorDescriptorId computeOpId : computeOperatorIds) {
                        computeLocations.addAll(info.getOperatorLocations().get(computeOpId));
                    }
                    System.out.println("job info " + info);

                } catch (Exception e) {
                    // TODO Add Exception handling here
                }
            }

            @Override
            public void jobletFinish(JobStatus status) {
                MetadataManager.INSTANCE.acquireWriteLatch();
                MetadataTransactionContext mdTxnCtx = null;
                try {
                    mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                    FeedDatasetDetails feedDetails = (FeedDatasetDetails) dataset.getDatasetDetails();
                    feedDetails.setFeedState(FeedState.INACTIVE);
                    feedDetails.setComputeNodes(null);
                    feedDetails.setIngestNodes(null);
                    MetadataManager.INSTANCE
                            .dropDataset(mdTxnCtx, dataset.getDataverseName(), dataset.getDatasetName());
                    MetadataManager.INSTANCE.addDataset(mdTxnCtx, dataset);
                } catch (Exception e) {

                }
            }

            public List<String> getIngestLocations() {
                return ingestLocations;
            }

            public List<String> getComputeLocations() {
                return computeLocations;
            }
        };
        return jobEventListener;
    }
}
