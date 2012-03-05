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
package edu.uci.ics.asterix.transaction.management.service.logging;

import java.util.ArrayList;
import java.util.List;

/*
 *  A generic filter that encompasses one or more filters (see @ILogFiler) that may be applied when selectively retrieving logs.
 *  The contained filters are assumed to form a conjunction.   
 */
public class GenericFilter implements ILogFilter {

    private final List<ILogFilter> logFilters;

    public GenericFilter() {
        logFilters = new ArrayList<ILogFilter>();
    }

    public GenericFilter(List<ILogFilter> logFilters) {
        this.logFilters = logFilters;
    }

    public boolean accept(IBuffer fileBuffer, long offset, int length) {
        boolean satisfies = true;
        for (ILogFilter logFilter : logFilters) {
            satisfies = satisfies && logFilter.accept(fileBuffer, offset, length);
            if (!satisfies) {
                break;
            }
        }
        return satisfies;
    }

    public void addFilter(ILogFilter logFilter) {
        logFilters.add(logFilter);
    }

    public boolean removeFilter(ILogFilter logFilter) {
        return logFilters.remove(logFilter);
    }
}
