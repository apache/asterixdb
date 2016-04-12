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
package org.apache.asterix.external.library;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.om.base.AMutableInt32;
import org.apache.asterix.om.base.AMutableString;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.functions.IExternalFunctionInfo;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;

public class RuntimeExternalFunctionUtil {

    private static Map<String, ClassLoader> libraryClassLoaders = new HashMap<String, ClassLoader>();

    public static void registerLibraryClassLoader(String dataverseName, String libraryName, ClassLoader classLoader) {
        String key = dataverseName + "." + libraryName;
        synchronized (libraryClassLoaders) {
            if (libraryClassLoaders.get(dataverseName) != null) {
                throw new IllegalStateException("library class loader already registered!");
            }
            libraryClassLoaders.put(key, classLoader);
        }
    }

    public static ClassLoader getLibraryClassLoader(String dataverseName, String libraryName) {
        String key = dataverseName + "." + libraryName;
        synchronized (libraryClassLoaders) {
            return libraryClassLoaders.get(key);
        }
    }

    public static IFunctionDescriptor getFunctionDescriptor(IFunctionInfo finfo) {
        switch (((IExternalFunctionInfo) finfo).getKind()) {
            case SCALAR:
                return getScalarFunctionDescriptor(finfo);
            case AGGREGATE:
            case UNNEST:
            case STATEFUL:
                throw new NotImplementedException("External " + finfo.getFunctionIdentifier().getName()
                        + " not supported");
        }
        return null;
    }

    private static AbstractScalarFunctionDynamicDescriptor getScalarFunctionDescriptor(IFunctionInfo finfo) {
        return new ExternalScalarFunctionDescriptor(finfo);
    }

    public static ByteBuffer allocateArgumentBuffers(IAType type) {
        switch (type.getTypeTag()) {
            case INT32:
                return ByteBuffer.allocate(4);
            case STRING:
                return ByteBuffer.allocate(32 * 1024);
            default:
                return ByteBuffer.allocate(32 * 1024);
        }
    }

    public static IAObject allocateArgumentObjects(IAType type) {
        switch (type.getTypeTag()) {
            case INT32:
                return new AMutableInt32(0);
            case STRING:
                return new AMutableString("");
            default:
                return null;
                /*
                ARecordType recordType = (ARecordType) type;
                IAType[] fieldTypes = recordType.getFieldTypes();
                IAObject[] fields = new IAObject[fieldTypes.length];
                for (int i = 0; i < fields.length; i++) {
                    fields[i] = allocateArgumentObjects(fieldTypes[i]);
                }
                return new AMutableRecord((ARecordType) type, fields);
                */
        }
    }

}
