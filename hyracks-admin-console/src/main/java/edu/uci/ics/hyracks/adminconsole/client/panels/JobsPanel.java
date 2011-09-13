package edu.uci.ics.hyracks.adminconsole.client.panels;

import java.util.ArrayList;
import java.util.List;

import com.google.gwt.core.client.GWT;
import com.google.gwt.core.client.JsArray;
import com.google.gwt.http.client.RequestException;
import com.google.gwt.uibinder.client.UiBinder;
import com.google.gwt.uibinder.client.UiField;
import com.google.gwt.user.client.Timer;
import com.google.gwt.user.client.Window;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.SplitLayoutPanel;
import com.google.gwt.user.client.ui.Widget;
import com.google.gwt.view.client.AsyncDataProvider;

import edu.uci.ics.hyracks.adminconsole.client.beans.JobSummary;
import edu.uci.ics.hyracks.adminconsole.client.rest.AbstractRestFunction;
import edu.uci.ics.hyracks.adminconsole.client.rest.GetJobSummariesFunction;
import edu.uci.ics.hyracks.adminconsole.client.widgets.JobsTableWidget;

public class JobsPanel extends Composite implements JobsTableWidget.IRefreshRequestHandler {
    interface Binder extends UiBinder<Widget, JobsPanel> {
    }

    private final static Binder binder = GWT.create(Binder.class);

    @UiField
    SplitLayoutPanel split;

    @UiField
    JobsTableWidget jobs;

    @UiField
    Widget details;

    private int callCounter;

    public JobsPanel() {
        initWidget(binder.createAndBindUi(this));

        jobs.setRefreshRequestHandler(this);

        Timer timer = new Timer() {
            @Override
            public void run() {
                refresh();
            }
        };
        refresh();
        timer.scheduleRepeating(5000);
    }

    @Override
    public void refresh() {
        try {
            final int counter = ++callCounter;
            GetJobSummariesFunction.INSTANCE.call(new AbstractRestFunction.ResultCallback<JsArray<JobSummary>>() {
                @Override
                public void onSuccess(JsArray<JobSummary> result) {
                    if (counter == callCounter) {
                        AsyncDataProvider<JobSummary> dataProvider = jobs.getDataProvider();
                        List<JobSummary> data = new ArrayList<JobSummary>();
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