/*
 * Copyright 2009-2013 by The Regents of the University of California
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
package edu.uci.ics.hyracks.control.cc.adminconsole;

import org.apache.wicket.Page;
import org.apache.wicket.protocol.http.WebApplication;

import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.cc.adminconsole.pages.IndexPage;

public class HyracksAdminConsoleApplication extends WebApplication {
    private ClusterControllerService ccs;

    @Override
    public void init() {
        ccs = (ClusterControllerService) getServletContext().getAttribute(ClusterControllerService.class.getName());
    }

    @Override
    public Class<? extends Page> getHomePage() {
        return IndexPage.class;
    }

    public ClusterControllerService getClusterControllerService() {
        return ccs;
    }
}