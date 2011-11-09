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
package edu.uci.ics.hyracks.control.cc.adminconsole.pages;

import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.request.mapper.parameter.PageParameters;
import org.apache.wicket.util.string.StringValue;

import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.cc.work.GetJobActivityGraphJSONWork;
import edu.uci.ics.hyracks.control.cc.work.GetJobRunJSONWork;
import edu.uci.ics.hyracks.control.cc.work.GetJobSpecificationJSONWork;

public class JobDetailsPage extends AbstractPage {
    private static final long serialVersionUID = 1L;

    public JobDetailsPage(PageParameters params) throws Exception {
        ClusterControllerService ccs = getAdminConsoleApplication().getClusterControllerService();

        StringValue jobIdStr = params.get("job-id");

        JobId jobId = JobId.parse(jobIdStr.toString());

        GetJobSpecificationJSONWork gjsw = new GetJobSpecificationJSONWork(ccs, jobId);
        ccs.getWorkQueue().scheduleAndSync(gjsw);
        add(new Label("job-specification", gjsw.getJSON().toString()));

        GetJobActivityGraphJSONWork gjagw = new GetJobActivityGraphJSONWork(ccs, jobId);
        ccs.getWorkQueue().scheduleAndSync(gjagw);
        add(new Label("job-activity-graph", gjagw.getJSON().toString()));

        GetJobRunJSONWork gjrw = new GetJobRunJSONWork(ccs, jobId);
        ccs.getWorkQueue().scheduleAndSync(gjrw);
        add(new Label("job-run", gjrw.getJSON().toString()));
    }
}