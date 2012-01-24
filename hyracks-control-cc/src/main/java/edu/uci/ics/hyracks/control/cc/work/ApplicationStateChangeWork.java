package edu.uci.ics.hyracks.control.cc.work;

import java.util.Set;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.cc.application.CCApplicationContext;
import edu.uci.ics.hyracks.control.common.application.ApplicationStatus;
import edu.uci.ics.hyracks.control.common.ipc.ClusterControllerFunctions;
import edu.uci.ics.hyracks.control.common.work.AbstractWork;
import edu.uci.ics.hyracks.control.common.work.IResultCallback;

public class ApplicationStateChangeWork extends AbstractWork {
    private static final Logger LOGGER = Logger.getLogger(ApplicationStateChangeWork.class.getName());

    private final ClusterControllerService ccs;
    private final ClusterControllerFunctions.ApplicationStateChangeResponseFunction ascrf;

    public ApplicationStateChangeWork(ClusterControllerService ccs,
            ClusterControllerFunctions.ApplicationStateChangeResponseFunction ascrf) {
        this.ccs = ccs;
        this.ascrf = ascrf;
    }

    @Override
    public void run() {
        final CCApplicationContext appCtx = ccs.getApplicationMap().get(ascrf.getApplicationName());
        if (appCtx == null) {
            LOGGER.warning("Got ApplicationStateChangeResponse for application " + ascrf.getApplicationName()
                    + " that does not exist");
            return;
        }
        switch (ascrf.getStatus()) {
            case INITIALIZED: {
                Set<String> pendingNodeIds = appCtx.getInitializationPendingNodeIds();
                boolean changed = pendingNodeIds.remove(ascrf.getNodeId());
                if (!changed) {
                    LOGGER.warning("Got ApplicationStateChangeResponse for application " + ascrf.getApplicationName()
                            + " from unexpected node " + ascrf.getNodeId() + " to state " + ascrf.getStatus());
                    return;
                }
                if (pendingNodeIds.isEmpty()) {
                    appCtx.setStatus(ApplicationStatus.INITIALIZED);
                    IResultCallback<Object> callback = appCtx.getInitializationCallback();
                    appCtx.setInitializationCallback(null);
                    callback.setValue(null);
                }
                return;
            }

            case DEINITIALIZED: {
                Set<String> pendingNodeIds = appCtx.getDeinitializationPendingNodeIds();
                boolean changed = pendingNodeIds.remove(ascrf.getNodeId());
                if (!changed) {
                    LOGGER.warning("Got ApplicationStateChangeResponse for application " + ascrf.getApplicationName()
                            + " from unexpected node " + ascrf.getNodeId() + " to state " + ascrf.getStatus());
                    return;
                }
                if (pendingNodeIds.isEmpty()) {
                    appCtx.setStatus(ApplicationStatus.DEINITIALIZED);
                    ccs.getExecutor().execute(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                appCtx.deinitialize();
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                            ccs.getWorkQueue().schedule(new AbstractWork() {
                                @Override
                                public void run() {
                                    ccs.getApplicationMap().remove(ascrf.getApplicationName());
                                    IResultCallback<Object> callback = appCtx.getDeinitializationCallback();
                                    appCtx.setDeinitializationCallback(null);
                                    callback.setValue(null);
                                }
                            });
                        }
                    });
                }
                return;
            }
        }
    }
}