package edu.uci.ics.hyracks.adminconsole.client.panels;

import com.google.gwt.core.client.GWT;
import com.google.gwt.http.client.RequestException;
import com.google.gwt.uibinder.client.UiBinder;
import com.google.gwt.uibinder.client.UiField;
import com.google.gwt.user.client.Timer;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.Widget;

import edu.uci.ics.hyracks.adminconsole.client.beans.NodeDetails;
import edu.uci.ics.hyracks.adminconsole.client.details.node.charts.MemoryUsageChart;
import edu.uci.ics.hyracks.adminconsole.client.details.node.charts.SystemLoadAverageChart;
import edu.uci.ics.hyracks.adminconsole.client.details.node.charts.ThreadCountChart;
import edu.uci.ics.hyracks.adminconsole.client.rest.AbstractRestFunction;
import edu.uci.ics.hyracks.adminconsole.client.rest.GetNodeDetailsFunction;

public class NodeControllerDetailsPanel extends Composite {
    interface Binder extends UiBinder<Widget, NodeControllerDetailsPanel> {
    }

    private final static Binder binder = GWT.create(Binder.class);

    private final String nodeId;

    @UiField
    MemoryUsageChart heapUsage;

    @UiField
    MemoryUsageChart nonheapUsage;

    @UiField
    SystemLoadAverageChart loadAverage;

    @UiField
    ThreadCountChart threadCount;

    private int callCounter;

    private Timer timer;

    public NodeControllerDetailsPanel(String nodeId) {
        initWidget(binder.createAndBindUi(this));

        this.nodeId = nodeId;
        heapUsage.setPrefix("Heap ");
        nonheapUsage.setPrefix("Non-Heap ");

        timer = new Timer() {
            @Override
            public void run() {
                refresh();
            }
        };
        refresh();
        timer.scheduleRepeating(10000);
    }

    public void destroy() {
        timer.cancel();
    }

    private void refresh() {
        try {
            final int counter = ++callCounter;
            new GetNodeDetailsFunction(nodeId).call(new AbstractRestFunction.ResultCallback<NodeDetails>() {
                @Override
                public void onSuccess(NodeDetails result) {
                    if (counter == callCounter) {
                        loadAverage.reset(result.getRRDPtr(), result.getHeartbeatTimes(),
                                result.getSystemLoadAverages());
                        heapUsage.reset(result.getRRDPtr(), result.getHeartbeatTimes(), result.getHeapInitSizes(),
                                result.getHeapUsedSizes(), result.getHeapCommittedSizes(), result.getHeapMaxSizes());
                        nonheapUsage.reset(result.getRRDPtr(), result.getHeartbeatTimes(),
                                result.getNonHeapInitSizes(), result.getNonHeapUsedSizes(),
                                result.getNonHeapCommittedSizes(), result.getNonHeapMaxSizes());
                        threadCount.reset(result.getRRDPtr(), result.getHeartbeatTimes(), result.getThreadCounts(),
                                result.getPeakThreadCounts());
                    }
                }

                @Override
                public void onError(Throwable exception) {

                }
            });
        } catch (RequestException e) {
            e.printStackTrace();
        }
    }
}