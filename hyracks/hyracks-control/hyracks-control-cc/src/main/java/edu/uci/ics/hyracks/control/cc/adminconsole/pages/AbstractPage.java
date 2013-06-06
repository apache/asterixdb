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
package edu.uci.ics.hyracks.control.cc.adminconsole.pages;

import org.apache.wicket.markup.html.WebPage;

import edu.uci.ics.hyracks.control.cc.adminconsole.HyracksAdminConsoleApplication;

public class AbstractPage extends WebPage {
    private static final long serialVersionUID = 1L;
    
    public HyracksAdminConsoleApplication getAdminConsoleApplication() {
        return (HyracksAdminConsoleApplication) getApplication();
    }
}