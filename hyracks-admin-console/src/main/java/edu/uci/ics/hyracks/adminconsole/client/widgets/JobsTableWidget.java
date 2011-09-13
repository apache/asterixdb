package edu.uci.ics.hyracks.adminconsole.client.widgets;

import com.google.gwt.core.client.GWT;
import com.google.gwt.uibinder.client.UiBinder;
import com.google.gwt.uibinder.client.UiField;
import com.google.gwt.user.cellview.client.CellTable;
import com.google.gwt.user.cellview.client.TextColumn;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.Widget;
import com.google.gwt.view.client.AsyncDataProvider;
import com.google.gwt.view.client.HasData;

import edu.uci.ics.hyracks.adminconsole.client.beans.JobSummary;

public class JobsTableWidget extends Composite {
    public interface IRefreshRequestHandler {
        public void refresh();
    }

    interface Binder extends UiBinder<Widget, JobsTableWidget> {
    }

    private final static Binder binder = GWT.create(Binder.class);

    @UiField
    CellTable<JobSummary> table;

    private AsyncDataProvider<JobSummary> jobSummaryProvider;

    private IRefreshRequestHandler refreshRequestHandler;

    public JobsTableWidget() {
        initWidget(binder.createAndBindUi(this));

        TextColumn<JobSummary> idCol = new TextColumn<JobSummary>() {
            @Override
            public String getValue(JobSummary object) {
                return object.getJobId();
            }
        };
        idCol.setSortable(true);

        TextColumn<JobSummary> appCol = new TextColumn<JobSummary>() {
            @Override
            public String getValue(JobSummary object) {
                return object.getApplicationName();
            }
        };
        appCol.setSortable(true);

        TextColumn<JobSummary> statusCol = new TextColumn<JobSummary>() {
            @Override
            public String getValue(JobSummary object) {
                return object.getStatus();
            }
        };
        statusCol.setSortable(true);

        /*
        TextColumn<JobSummary> createTimeCol = new TextColumn<JobSummary>() {
            @Override
            public String getValue(JobSummary object) {
                return renderTime(object.getCreateTime());
            }
        };
        createTimeCol.setSortable(true);

        TextColumn<JobSummary> startTimeCol = new TextColumn<JobSummary>() {
            @Override
            public String getValue(JobSummary object) {
                return renderTime(object.getStartTime());
            }
        };
        startTimeCol.setSortable(true);

        TextColumn<JobSummary> endTimeCol = new TextColumn<JobSummary>() {
            @Override
            public String getValue(JobSummary object) {
                return renderTime(object.getEndTime());
            }
        };
        endTimeCol.setSortable(true);
        */
        table.addColumn(idCol, "Job Id");
        table.addColumn(appCol, "Application Name");
        table.addColumn(statusCol, "Status");
        /*
        table.addColumn(createTimeCol, "Created At");
        table.addColumn(startTimeCol, "Started At");
        table.addColumn(endTimeCol, "Finished At");
        */

        jobSummaryProvider = new AsyncDataProvider<JobSummary>(JobSummary.KEY_PROVIDER) {
            @Override
            protected void onRangeChanged(HasData<JobSummary> display) {
                if (refreshRequestHandler != null) {
                    refreshRequestHandler.refresh();
                }
            }
        };
        jobSummaryProvider.addDataDisplay(table);
    }

    public AsyncDataProvider<JobSummary> getDataProvider() {
        return jobSummaryProvider;
    }

    public void setRefreshRequestHandler(IRefreshRequestHandler refreshRequestHandler) {
        this.refreshRequestHandler = refreshRequestHandler;
    }

    private static String renderTime(Long time) {
        if (time < 0) {
            return "";
        }
        return time.toString();
    }
}