package edu.uci.ics.hyracks.control.cc.job.manager;

import java.util.Set;
import java.util.UUID;

import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.cc.job.JobRun;
import edu.uci.ics.hyracks.control.cc.job.manager.events.JobAttemptStartEvent;
import edu.uci.ics.hyracks.control.cc.remote.RemoteRunner;
import edu.uci.ics.hyracks.control.cc.remote.ops.JobletAborter;

public class JobLifecycleHelper {
    public static void abortJob(final ClusterControllerService ccs, final UUID jobId, final int attempt,
            final Set<String> targetNodes) {
        JobRun run = ccs.getRunMap().get(jobId);
        if (run != null && run.registerAbort(attempt)) {
            ccs.getExecutor().execute(new Runnable() {
                @Override
                public void run() {
                    if (!targetNodes.isEmpty()) {
                        JobletAborter[] jas = new JobletAborter[targetNodes.size()];
                        int i = 0;
                        for (String nodeId : targetNodes) {
                            jas[i++] = new JobletAborter(nodeId, jobId, attempt);
                        }
                        try {
                            RemoteRunner.runRemote(ccs, jas, null);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    ccs.getJobQueue().schedule(new JobAttemptStartEvent(ccs, jobId));
                }
            });
        }
    }
}