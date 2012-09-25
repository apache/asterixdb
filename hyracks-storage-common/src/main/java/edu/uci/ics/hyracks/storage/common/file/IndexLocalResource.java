/*
 * Copyright 2009-2012 by The Regents of the University of California
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
package edu.uci.ics.hyracks.storage.common.file;

public class IndexLocalResource implements ILocalResource {
    private static final long serialVersionUID = -8638308062094620884L;
    private final long resourceId;
    private final String resourceName;
    private final Object object;
    private final ILocalResourceClass resourceClass;

    public IndexLocalResource(long resourceId, String resourceName, Object object, ILocalResourceClass resourceClass) {
        this.resourceId = resourceId;
        this.resourceName = resourceName;
        this.object = object;
        this.resourceClass = resourceClass;
    }

    @Override
    public long getResourceId() {
        return resourceId;
    }

    @Override
    public String getResourceName() {
        return resourceName;
    }

    @Override
    public Object getResourceObject() {
        return object;
    }

    @Override
    public ILocalResourceClass getResourceClass() {
        return resourceClass;
    }

}
