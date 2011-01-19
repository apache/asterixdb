package edu.uci.ics.hyracks.control.cc.job.manager;

import java.util.Set;
import java.util.UUID;

import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.cc.job.manager.events.JobAttemptStartEvent;
import edu.uci.ics.hyracks.control.cc.remote.RemoteRunner;
import edu.uci.ics.hyracks.control.cc.remote.ops.JobletAborter;

public class JobLifecycleHelper {
    public static void abortJob(ClusterControllerService ccs, UUID jobId, Set<String> targetNodes) {
        if (!targetNodes.isEmpty()) {
            JobletAborter[] jas = new JobletAborter[targetNodes.size()];
            int i = 0;
            for (String nodeId : targetNodes) {
                jas[i++] = new JobletAborter(nodeId, jobId);
            }
            try {
                RemoteRunner.runRemote(ccs, jas, null);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        ccs.getJobQueue().schedule(new JobAttemptStartEvent(ccs, jobId));
    }
}