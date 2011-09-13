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
package edu.uci.ics.hyracks.adminconsole.client;

import com.google.gwt.core.client.EntryPoint;
import com.google.gwt.core.client.GWT;
import com.google.gwt.uibinder.client.UiBinder;
import com.google.gwt.uibinder.client.UiField;
import com.google.gwt.user.client.Window;
import com.google.gwt.user.client.ui.DockLayoutPanel;
import com.google.gwt.user.client.ui.RootLayoutPanel;

import edu.uci.ics.hyracks.adminconsole.client.connection.ServerConnection;
import edu.uci.ics.hyracks.adminconsole.client.panels.ApplicationsPanel;
import edu.uci.ics.hyracks.adminconsole.client.panels.DashboardPanel;
import edu.uci.ics.hyracks.adminconsole.client.panels.JobsPanel;
import edu.uci.ics.hyracks.adminconsole.client.panels.NodeControllersPanel;
import edu.uci.ics.hyracks.adminconsole.client.panels.TopPanel;

public class HyracksAdminConsole implements EntryPoint {
    public static HyracksAdminConsole INSTANCE;

    private final Messages messages = GWT.create(Messages.class);

    private ServerConnection serverConnection;

    @UiField
    TopPanel topPanel;

    @UiField
    DashboardPanel dashboardPanel;

    @UiField
    ApplicationsPanel applicationsPanel;

    @UiField
    NodeControllersPanel nodeControllersPanel;

    @UiField
    JobsPanel jobsPanel;

    interface Binder extends UiBinder<DockLayoutPanel, HyracksAdminConsole> {
    }

    private final static Binder binder = GWT.create(Binder.class);

    public ServerConnection getServerConnection() {
        return serverConnection;
    }

    public void onModuleLoad() {
        INSTANCE = this;
        serverConnection = new ServerConnection();
        String serverURLPrefix = Window.Location.getParameter("cclocation");
        serverConnection.setServerURLPrefix(serverURLPrefix);

        DockLayoutPanel lp = binder.createAndBindUi(this);

        // Get rid of scrollbars, and clear out the window's built-in margin,
        // because we want to take advantage of the entire client area.
        Window.enableScrolling(false);
        Window.setMargin("0px");

        // Add the outer panel to the RootLayoutPanel, so that it will be
        // displayed.
        RootLayoutPanel root = RootLayoutPanel.get();
        root.add(lp);
    }
}