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
package edu.uci.ics.hyracks.dataflow.std.map;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class ReflectionBasedDeserializedMapperFactory implements IDeserializedMapperFactory {
    private static final long serialVersionUID = 1L;

    private final Class<? extends IDeserializedMapper> mapperClass;

    public ReflectionBasedDeserializedMapperFactory(Class<? extends IDeserializedMapper> mapperClass) {
        this.mapperClass = mapperClass;
    }

    @Override
    public IDeserializedMapper createMapper() throws HyracksDataException {
        try {
            return mapperClass.newInstance();
        } catch (InstantiationException e) {
            throw new HyracksDataException(e);
        } catch (IllegalAccessException e) {
            throw new HyracksDataException(e);
        }
    }
}