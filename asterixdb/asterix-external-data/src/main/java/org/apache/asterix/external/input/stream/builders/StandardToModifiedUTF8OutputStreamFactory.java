/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.external.input.stream.builders;

import org.apache.asterix.dataflow.data.nontagged.serde.AStringSerializerDeserializer;
import org.apache.asterix.external.input.stream.StandardUTF8ToModifiedUTF8OutputStream;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.util.container.IObjectFactory;
import org.apache.hyracks.util.string.UTF8StringReader;
import org.apache.hyracks.util.string.UTF8StringWriter;

public class StandardToModifiedUTF8OutputStreamFactory
        implements IObjectFactory<StandardUTF8ToModifiedUTF8OutputStream, ATypeTag> {

    @Override
    public StandardUTF8ToModifiedUTF8OutputStream create(ATypeTag type) {
        return new StandardUTF8ToModifiedUTF8OutputStream(
                new AStringSerializerDeserializer(new UTF8StringWriter(), new UTF8StringReader()));
    }

}
