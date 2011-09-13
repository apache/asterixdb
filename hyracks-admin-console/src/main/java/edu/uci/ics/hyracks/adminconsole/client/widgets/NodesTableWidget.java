/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.adminconsole.client.widgets;

import com.google.gwt.cell.client.ClickableTextCell;
import com.google.gwt.cell.client.FieldUpdater;
import com.google.gwt.core.client.GWT;
import com.google.gwt.uibinder.client.UiBinder;
import com.google.gwt.uibinder.client.UiField;
import com.google.gwt.user.cellview.client.CellTable;
import com.google.gwt.user.cellview.client.Column;
import com.google.gwt.user.cellview.client.TextColumn;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.Widget;
import com.google.gwt.view.client.AsyncDataProvider;
import com.google.gwt.view.client.HasData;

import edu.uci.ics.hyracks.adminconsole.client.beans.NodeSummary;

public class NodesTableWidget extends Composite {
    public interface IRefreshRequestHandler {
        public void refresh();
    }

    public interface IClickListener {
        public void click(String nodeId);
    }

    interface Binder extends UiBinder<Widget, NodesTableWidget> {
    }

    private final static Binder binder = GWT.create(Binder.class);

    @UiField
    CellTable<NodeSummary> table;

    private AsyncDataProvider<NodeSummary> nodeSummaryProvider;

    private IRefreshRequestHandler refreshRequestHandler;

    private IClickListener cl;

    public NodesTableWidget() {
        initWidget(binder.createAndBindUi(this));

        Column<NodeSummary, String> idCol = new Column<NodeSummary, String>(new ClickableTextCell()) {
            @Override
            public String getValue(NodeSummary object) {
                return object.getNodeId();
            }
        };
        idCol.setFieldUpdater(new FieldUpdater<NodeSummary, String>() {
            @Override
            public void update(int index, NodeSummary object, String value) {
                if (cl != null) {
                    cl.click(value);
                }
            }
        });
        idCol.setSortable(true);

        TextColumn<NodeSummary> heapUsedCol = new TextColumn<NodeSummary>() {
            @Override
            public String getValue(NodeSummary object) {
                return String.valueOf(object.getHeapUsed());
            }
        };
        heapUsedCol.setSortable(true);

        TextColumn<NodeSummary> systemLoadAvgCol = new TextColumn<NodeSummary>() {
            @Override
            public String getValue(NodeSummary object) {
                return String.valueOf(object.getSystemLoadAverage());
            }
        };
        systemLoadAvgCol.setSortable(true);

        table.addColumn(idCol, "Node Id");
        table.addColumn(heapUsedCol, "Heap Used");
        table.addColumn(systemLoadAvgCol, "System Load Average");

        nodeSummaryProvider = new AsyncDataProvider<NodeSummary>(NodeSummary.KEY_PROVIDER) {
            @Override
            protected void onRangeChanged(HasData<NodeSummary> display) {
                if (refreshRequestHandler != null) {
                    refreshRequestHandler.refresh();
                }
            }
        };
        nodeSummaryProvider.addDataDisplay(table);
    }

    public void setClickListener(IClickListener cl) {
        this.cl = cl;
    }

    public AsyncDataProvider<NodeSummary> getDataProvider() {
        return nodeSummaryProvider;
    }

    public void setRefreshRequestHandler(IRefreshRequestHandler refreshRequestHandler) {
        this.refreshRequestHandler = refreshRequestHandler;
    }
}