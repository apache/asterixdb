package edu.uci.ics.asterix.external.library;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.asterix.om.base.AMutableInt32;
import edu.uci.ics.asterix.om.base.AMutableRecord;
import edu.uci.ics.asterix.om.base.AMutableString;
import edu.uci.ics.asterix.om.base.IAObject;
import edu.uci.ics.asterix.om.functions.IExternalFunctionInfo;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.types.ARecordType;
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

    public static File getExternalLibraryDeployDir(String nodeId) {
        String filePath = null;
        if (nodeId != null) {
            filePath = "edu.uci.ics.hyracks.control.nc.NodeControllerService" + "/" + nodeId + "/"
                    + "applications/asterix/expanded/external-lib/libraries";
        } else {
            filePath = "ClusterControllerService" + "/" + "applications/asterix/expanded/external-lib/libraries";

        }
        return new File(filePath);
    }
}
