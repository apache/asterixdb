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
package edu.uci.ics.hyracks.adminconsole.client.panels;

import com.google.gwt.core.client.GWT;
import com.google.gwt.http.client.RequestException;
import com.google.gwt.uibinder.client.UiBinder;
import com.google.gwt.user.client.Timer;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.Widget;

import edu.uci.ics.hyracks.adminconsole.client.beans.JobRunDetails;
import edu.uci.ics.hyracks.adminconsole.client.rest.AbstractRestFunction;
import edu.uci.ics.hyracks.adminconsole.client.rest.GetJobRunDetailsFunction;

public class JobsDetailsPanel extends Composite {
    interface Binder extends UiBinder<Widget, JobsDetailsPanel> {
    }

    private final static Binder binder = GWT.create(Binder.class);

    private final String jobId;

    private int callCounter;

    private Timer timer;

    public JobsDetailsPanel(String jobId) {
        initWidget(binder.createAndBindUi(this));
        this.jobId = jobId;

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
            new GetJobRunDetailsFunction(jobId).call(new AbstractRestFunction.ResultCallback<JobRunDetails>() {
                @Override
                public void onSuccess(JobRunDetails result) {
                    if (counter == callCounter) {
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