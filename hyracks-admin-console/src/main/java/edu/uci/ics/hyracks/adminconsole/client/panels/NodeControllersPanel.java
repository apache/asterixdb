package edu.uci.ics.hyracks.adminconsole.client.panels;

import java.util.ArrayList;
import java.util.List;

import com.google.gwt.core.client.GWT;
import com.google.gwt.core.client.JsArray;
import com.google.gwt.http.client.RequestException;
import com.google.gwt.uibinder.client.UiBinder;
import com.google.gwt.uibinder.client.UiField;
import com.google.gwt.user.client.Timer;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.SplitLayoutPanel;
import com.google.gwt.user.client.ui.Widget;
import com.google.gwt.view.client.AsyncDataProvider;

import edu.uci.ics.hyracks.adminconsole.client.beans.NodeSummary;
import edu.uci.ics.hyracks.adminconsole.client.rest.AbstractRestFunction;
import edu.uci.ics.hyracks.adminconsole.client.rest.GetNodeSummariesFunction;
import edu.uci.ics.hyracks.adminconsole.client.widgets.NodesTableWidget;

public class NodeControllersPanel extends Composite implements NodesTableWidget.IRefreshRequestHandler {
    interface Binder extends UiBinder<Widget, NodeControllersPanel> {
    }

    private final static Binder binder = GWT.create(Binder.class);

    @UiField
    SplitLayoutPanel split;

    @UiField
    NodesTableWidget nodes;

    @UiField
    Widget details;

    private int callCounter;

    public NodeControllersPanel() {
        initWidget(binder.createAndBindUi(this));

        nodes.setRefreshRequestHandler(this);
        nodes.setClickListener(new NodesTableWidget.IClickListener() {
            @Override
            public void click(String nodeId) {
                if (details instanceof NodeControllerDetailsPanel) {
                    ((NodeControllerDetailsPanel) details).destroy();
                }
                split.remove(details);
                details = new NodeControllerDetailsPanel(nodeId);
                split.add(details);
            }
        });

        Timer timer = new Timer() {
            @Override
            public void run() {
                refresh();
            }
        };
        refresh();
        timer.scheduleRepeating(10000);
    }

    @Override
    public void refresh() {
        try {
            final int counter = ++callCounter;
            GetNodeSummariesFunction.INSTANCE.call(new AbstractRestFunction.ResultCallback<JsArray<NodeSummary>>() {
                @Override
                public void onSuccess(JsArray<NodeSummary> result) {
                    if (counter == callCounter) {
                        AsyncDataProvider<NodeSummary> dataProvider = nodes.getDataProvider();
                        List<NodeSummary> data = new ArrayList<NodeSummary>();
                        for (int i = 0; i < result.length(); ++i) {
                            data.add(result.get(i));
                        }
                        dataProvider.updateRowData(0, data);
                        dataProvider.updateRowCount(result.length(), true);
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