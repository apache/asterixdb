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
package edu.uci.ics.asterix.external.library;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.asterix.om.base.AMutableInt32;
import edu.uci.ics.asterix.om.base.AMutableString;
import edu.uci.ics.asterix.om.base.IAObject;
import edu.uci.ics.asterix.om.functions.IExternalFunctionInfo;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.IFunctionInfo;

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
