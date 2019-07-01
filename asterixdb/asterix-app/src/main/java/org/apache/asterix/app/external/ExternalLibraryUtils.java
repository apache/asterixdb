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
package org.apache.asterix.app.external;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.library.ILibraryManager;
import org.apache.asterix.external.api.IDataSourceAdapter;
import org.apache.asterix.external.dataset.adapter.AdapterIdentifier;
import org.apache.asterix.external.library.ExternalLibrary;
import org.apache.asterix.external.library.LibraryAdapter;
import org.apache.asterix.external.library.LibraryFunction;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.entities.DatasourceAdapter;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.asterix.metadata.entities.Function;
import org.apache.asterix.metadata.entities.Library;
import org.apache.asterix.metadata.utils.MetadataUtil;
import org.apache.asterix.runtime.formats.NonTaggedDataFormat;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ExternalLibraryUtils {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final FilenameFilter nonHiddenFileNameFilter = (dir, name) -> !name.startsWith(".");

    private ExternalLibraryUtils() {
    }

    public static void setUpExternaLibrary(ILibraryManager externalLibraryManager, boolean isMetadataNode,
            String libraryPath) throws Exception {
        // start by un-installing removed libraries (Metadata Node only)
        Map<String, List<String>> uninstalledLibs = null;
        if (isMetadataNode) {
            uninstalledLibs = uninstallLibraries();
        }

        // get the directory of the to be installed libraries
        String[] pathSplit = libraryPath.split("\\.");
        String[] dvSplit = pathSplit[pathSplit.length - 2].split("/");
        String dataverse = dvSplit[dvSplit.length - 1];
        String name = pathSplit[pathSplit.length - 1].trim();
        File installLibDir = new File(libraryPath);

        // directory exists?
        if (installLibDir.exists()) {
            registerClassLoader(externalLibraryManager, dataverse, name, libraryPath);
            configureLibrary(externalLibraryManager, dataverse, name, installLibDir, uninstalledLibs, isMetadataNode);
        }
    }

    public static void setUpInstalledLibraries(ILibraryManager externalLibraryManager, boolean isMetadataNode,
            File appDir) throws Exception {
        File[] libs = appDir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return dir.isDirectory();
            }
        });
        if (libs != null) {
            for (File lib : libs) {
                setUpExternaLibrary(externalLibraryManager, isMetadataNode, lib.getAbsolutePath());
            }
        }
    }

    /**
     * un-install libraries.
     *
     * @return a map from dataverse -> list of uninstalled libraries.
     * @throws Exception
     */
    private static Map<String, List<String>> uninstallLibraries() throws Exception {
        Map<String, List<String>> uninstalledLibs = new HashMap<>();
        // get the directory of the un-install libraries
        File uninstallLibDir = getLibraryUninstallDir();
        String[] uninstallLibNames;
        // directory exists?
        if (uninstallLibDir.exists()) {
            // list files
            uninstallLibNames = uninstallLibDir.list(nonHiddenFileNameFilter);
            for (String uninstallLibName : uninstallLibNames) {
                // Get the <dataverse name - library name> pair
                String[] components = uninstallLibName.split("\\.");
                String dataverse = components[0];
                String libName = components[1];
                // un-install
                uninstallLibrary(dataverse, libName);
                // delete the library file
                new File(uninstallLibDir, uninstallLibName).delete();
                // add the library to the list of uninstalled libraries
                List<String> uinstalledLibsInDv = uninstalledLibs.get(dataverse);
                if (uinstalledLibsInDv == null) {
                    uinstalledLibsInDv = new ArrayList<>();
                    uninstalledLibs.put(dataverse, uinstalledLibsInDv);
                }
                uinstalledLibsInDv.add(libName);
            }
        }
        return uninstalledLibs;
    }

    /**
     * Remove the library from metadata completely.
     * TODO Currently, external libraries only include functions and adapters. we need to extend this to include:
     * 1. external data source
     * 2. data parser
     *
     * @param dataverse
     * @param libraryName
     * @return true if the library was found and removed, false otherwise
     * @throws AsterixException
     * @throws RemoteException
     * @throws ACIDException
     */
    public static boolean uninstallLibrary(String dataverse, String libraryName)
            throws AsterixException, RemoteException, ACIDException {
        MetadataTransactionContext mdTxnCtx = null;
        try {
            // begin transaction
            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            // make sure dataverse exists
            Dataverse dv = MetadataManager.INSTANCE.getDataverse(mdTxnCtx, dataverse);
            if (dv == null) {
                return false;
            }
            // make sure library exists
            Library library = MetadataManager.INSTANCE.getLibrary(mdTxnCtx, dataverse, libraryName);
            if (library == null) {
                return false;
            }

            // get dataverse functions
            List<Function> functions = MetadataManager.INSTANCE.getDataverseFunctions(mdTxnCtx, dataverse);
            for (Function function : functions) {
                // does function belong to library?
                if (function.getName().startsWith(libraryName + "#")) {
                    // drop the function
                    MetadataManager.INSTANCE.dropFunction(mdTxnCtx,
                            new FunctionSignature(dataverse, function.getName(), function.getArity()));
                }
            }

            // get the dataverse adapters
            List<DatasourceAdapter> adapters = MetadataManager.INSTANCE.getDataverseAdapters(mdTxnCtx, dataverse);
            for (DatasourceAdapter adapter : adapters) {
                // belong to the library?
                if (adapter.getAdapterIdentifier().getName().startsWith(libraryName + "#")) {
                    // remove adapter <! we didn't check if there are feeds which use this adapter>
                    MetadataManager.INSTANCE.dropAdapter(mdTxnCtx, dataverse, adapter.getAdapterIdentifier().getName());
                }
            }
            // drop the library itself
            MetadataManager.INSTANCE.dropLibrary(mdTxnCtx, dataverse, libraryName);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            MetadataManager.INSTANCE.abortTransaction(mdTxnCtx);
            throw new AsterixException(e);
        }
        return true;
    }

    private static void addLibraryToMetadata(Map<String, List<String>> uninstalledLibs, String dataverse,
            String libraryName, ExternalLibrary library) throws ACIDException, RemoteException {
        // Modify metadata accordingly
        List<String> uninstalledLibsInDv = uninstalledLibs.get(dataverse);
        // was this library just un-installed?
        boolean wasUninstalled = uninstalledLibsInDv != null && uninstalledLibsInDv.contains(libraryName);
        MetadataTransactionContext mdTxnCtx = null;
        try {
            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            Library libraryInMetadata = MetadataManager.INSTANCE.getLibrary(mdTxnCtx, dataverse, libraryName);
            if (libraryInMetadata != null && !wasUninstalled) {
                // exists in metadata and was not un-installed, we return.
                // Another place which shows that our metadata transactions are broken
                // (we didn't call commit before!!!)
                MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                return;
            }

            // Add library
            MetadataManager.INSTANCE.addLibrary(mdTxnCtx, new Library(dataverse, libraryName));
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Added library " + libraryName + " to Metadata");
            }

            // Get the dataverse
            Dataverse dv = MetadataManager.INSTANCE.getDataverse(mdTxnCtx, dataverse);
            if (dv == null) {
                MetadataManager.INSTANCE.addDataverse(mdTxnCtx, new Dataverse(dataverse,
                        NonTaggedDataFormat.NON_TAGGED_DATA_FORMAT, MetadataUtil.PENDING_NO_OP));
            }
            // Add functions
            if (library.getLibraryFunctions() != null) {
                for (LibraryFunction function : library.getLibraryFunctions().getLibraryFunction()) {
                    String[] fargs = function.getArgumentType().trim().split(",");
                    String functionFullName = getExternalFunctionFullName(libraryName, function.getName().trim());
                    String functionReturnType = function.getReturnType().trim();
                    String functionDefinition = function.getDefinition().trim();
                    String functionLanguage = library.getLanguage().trim();
                    String functionType = function.getFunctionType().trim();
                    List<String> args = new ArrayList<>();
                    for (String arg : fargs) {
                        args.add(arg.trim());
                    }
                    FunctionSignature signature = new FunctionSignature(dataverse, functionFullName, args.size());
                    Function f = new Function(signature, args, functionReturnType, functionDefinition, functionLanguage,
                            functionType, null);
                    MetadataManager.INSTANCE.addFunction(mdTxnCtx, f);
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Installed function: " + functionFullName);
                    }
                }
            }

            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Installed functions in library :" + libraryName);
            }

            // Add adapters
            if (library.getLibraryAdapters() != null) {
                for (LibraryAdapter adapter : library.getLibraryAdapters().getLibraryAdapter()) {
                    String adapterFactoryClass = adapter.getFactoryClass().trim();
                    String adapterName = getExternalFunctionFullName(libraryName, adapter.getName().trim());
                    AdapterIdentifier aid = new AdapterIdentifier(dataverse, adapterName);
                    DatasourceAdapter dsa =
                            new DatasourceAdapter(aid, adapterFactoryClass, IDataSourceAdapter.AdapterType.EXTERNAL);
                    MetadataManager.INSTANCE.addAdapter(mdTxnCtx, dsa);
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Installed adapter: " + adapterName);
                    }
                }
            }

            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Installed adapters in library :" + libraryName);
            }
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            if (LOGGER.isErrorEnabled()) {
                LOGGER.log(Level.ERROR, "Exception in installing library " + libraryName, e);
            }
            MetadataManager.INSTANCE.abortTransaction(mdTxnCtx);
        }
    }

    /**
     * Each element of a library is installed as part of a transaction. Any
     * failure in installing an element does not effect installation of other
     * libraries.
     */
    protected static void configureLibrary(ILibraryManager libraryManager, String dataverse, String libraryName,
            final File libraryDir, Map<String, List<String>> uninstalledLibs, boolean isMetadataNode) throws Exception {

        String[] libraryDescriptors = libraryDir.list((dir, name) -> name.endsWith(".xml"));

        if (libraryDescriptors == null) {
            throw new IOException("Unable to list files in directory " + libraryDir);
        }

        if (libraryDescriptors.length > 1) {
            throw new IllegalStateException("More than 1 library descriptors defined");
        }

        // Prepare possible parameters
        ExternalLibrary library = getLibrary(new File(libraryDir + File.separator + libraryDescriptors[0]));
        if (library.getLibraryFunctions() != null) {
            library.getLibraryFunctions().getLibraryFunction().forEach(fun -> {
                if (fun.getParameters() != null) {
                    libraryManager.addFunctionParameters(dataverse,
                            getExternalFunctionFullName(libraryName, fun.getName()), fun.getParameters());
                }
            });
        }
        if (isMetadataNode) {
            addLibraryToMetadata(uninstalledLibs, dataverse, libraryName, library);
        }
    }

    /**
     * register the library class loader with the external library manager
     *
     * @param dataverse
     * @param libraryPath
     * @throws Exception
     */
    protected static void registerClassLoader(ILibraryManager externalLibraryManager, String dataverse, String name,
            String libraryPath) throws Exception {
        // get the class loader
        URLClassLoader classLoader = getLibraryClassLoader(dataverse, name, libraryPath);
        // register it with the external library manager
        externalLibraryManager.registerLibraryClassLoader(dataverse, name, classLoader);
    }

    /**
     * Get the library from the xml file
     *
     * @param libraryXMLPath
     * @return
     * @throws Exception
     */
    private static ExternalLibrary getLibrary(File libraryXMLPath) throws Exception {
        JAXBContext configCtx = JAXBContext.newInstance(ExternalLibrary.class);
        Unmarshaller unmarshaller = configCtx.createUnmarshaller();
        ExternalLibrary library = (ExternalLibrary) unmarshaller.unmarshal(libraryXMLPath);
        return library;
    }

    /**
     * Get the class loader for the library
     *
     * @param libraryPath
     * @param dataverse
     * @return
     * @throws Exception
     */
    private static URLClassLoader getLibraryClassLoader(String dataverse, String name, String libraryPath)
            throws Exception {
        // Get a reference to the library directory
        File installDir = new File(libraryPath);
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Installing lirbary " + name + " in dataverse " + dataverse + "." + " Install Directory: "
                    + installDir.getAbsolutePath());
        }

        // get a reference to the specific library dir
        File libDir = installDir;

        FilenameFilter jarFileFilter = new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".jar");
            }
        };

        // Get the jar file <Allow only a single jar file>
        String[] jarsInLibDir = libDir.list(jarFileFilter);
        if (jarsInLibDir.length > 1) {
            throw new Exception("Incorrect library structure: found multiple library jars");
        }
        if (jarsInLibDir.length <= 0) {
            throw new Exception("Incorrect library structure: could not find library jar");
        }

        File libJar = new File(libDir, jarsInLibDir[0]);
        // get the jar dependencies
        File libDependencyDir = new File(libDir.getAbsolutePath() + File.separator + "lib");
        int numDependencies = 1;
        String[] libraryDependencies = null;
        if (libDependencyDir.exists()) {
            libraryDependencies = libDependencyDir.list(jarFileFilter);
            numDependencies += libraryDependencies.length;
        }

        ClassLoader parentClassLoader = ExternalLibraryUtils.class.getClassLoader();
        URL[] urls = new URL[numDependencies];
        int count = 0;
        // get url of library
        urls[count++] = libJar.toURI().toURL();

        // get urls for dependencies
        if (libraryDependencies != null && libraryDependencies.length > 0) {
            for (String dependency : libraryDependencies) {
                File file = new File(libDependencyDir + File.separator + dependency);
                urls[count++] = file.toURI().toURL();
            }
        }

        if (LOGGER.isInfoEnabled()) {
            StringBuilder logMesg = new StringBuilder("Classpath for library " + dataverse + ": ");
            for (URL url : urls) {
                logMesg.append(url.getFile() + File.pathSeparatorChar);
            }
            LOGGER.info(logMesg.toString());
        }

        // create and return the class loader
        return new ExternalLibraryClassLoader(urls, parentClassLoader);
    }

    /**
     * @return the directory "System.getProperty("app.home", System.getProperty("user.home")/lib/udfs/uninstall"
     */
    protected static File getLibraryUninstallDir() {
        return new File(System.getProperty("app.home", System.getProperty("user.home")) + File.separator + "lib"
                + File.separator + "udfs" + File.separator + "uninstall");
    }

    public static String getExternalFunctionFullName(String libraryName, String functionName) {
        return libraryName + "#" + functionName;
    }

}
