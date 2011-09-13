package edu.uci.ics.hyracks.adminconsole.client.panels;

import com.google.gwt.core.client.GWT;
import com.google.gwt.uibinder.client.UiBinder;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.Widget;

public class ApplicationsPanel extends Composite {
    interface Binder extends UiBinder<Widget, ApplicationsPanel> {
    }

    private final static Binder binder = GWT.create(Binder.class);

    public ApplicationsPanel() {
        initWidget(binder.createAndBindUi(this));
    }
}