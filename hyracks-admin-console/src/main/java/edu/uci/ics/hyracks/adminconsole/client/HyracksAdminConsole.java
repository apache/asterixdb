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