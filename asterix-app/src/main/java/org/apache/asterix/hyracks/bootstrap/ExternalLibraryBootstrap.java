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
package org.apache.asterix.hyracks.bootstrap;

import java.io.File;
import java.io.FilenameFilter;
import java.net.URL;
import java.net.URLClassLoader;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.external.library.ExternalLibrary;
import org.apache.asterix.external.library.ExternalLibraryManager;
import org.apache.asterix.external.library.LibraryAdapter;
import org.apache.asterix.external.library.LibraryFunction;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.api.IMetadataEntity;
import org.apache.asterix.metadata.entities.DatasourceAdapter;
import org.apache.asterix.metadata.entities.DatasourceAdapter.AdapterType;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.asterix.metadata.entities.Library;
import org.apache.asterix.metadata.feeds.AdapterIdentifier;
import org.apache.asterix.runtime.formats.NonTaggedDataFormat;

public class ExternalLibraryBootstrap {

    private static Logger LOGGER = Logger.getLogger(ExternalLibraryBootstrap.class.getName());

    public static void setUpExternaLibraries(boolean isMetadataNode) throws Exception {

        Map<String, List<String>> uninstalledLibs = null;
        if (isMetadataNode) {
            uninstalledLibs = uninstallLibraries();
        }

        File installLibDir = getLibraryInstallDir();
        if (installLibDir.exists()) {
            for (String dataverse : installLibDir.list()) {
                File dataverseDir = new File(installLibDir, dataverse);
                String[] libraries = dataverseDir.list();
                for (String library : libraries) {
                    registerLibrary(dataverse, library, isMetadataNode, installLibDir);
                    if (isMetadataNode) {
                        File libraryDir = new File(installLibDir.getAbsolutePath() + File.separator + dataverse
                                + File.separator + library);
                        installLibraryIfNeeded(dataverse, libraryDir, uninstalledLibs);
                    }
                }
            }
        }
    }

    private static Map<String, List<String>> uninstallLibraries() throws Exception {
        Map<String, List<String>> uninstalledLibs = new HashMap<String, List<String>>();
        File uninstallLibDir = getLibraryUninstallDir();
        String[] uninstallLibNames;
        if (uninstallLibDir.exists()) {
            uninstallLibNames = uninstallLibDir.list();
            for (String uninstallLibName : uninstallLibNames) {
                String[] components = uninstallLibName.split("\\.");
                String dataverse = components[0];
                String libName = components[1];
                uninstallLibrary(dataverse, libName);
                new File(uninstallLibDir, uninstallLibName).delete();
                List<String> uinstalledLibsInDv = uninstalledLibs.get(dataverse);
                if (uinstalledLibsInDv == null) {
                    uinstalledLibsInDv = new ArrayList<String>();
                    uninstalledLibs.put(dataverse, uinstalledLibsInDv);
                }
                uinstalledLibsInDv.add(libName);
            }
        }
        return uninstalledLibs;
    }

    private static boolean uninstallLibrary(String dataverse, String libraryName)
            throws AsterixException, RemoteException, ACIDException {
        MetadataTransactionContext mdTxnCtx = null;
        try {
            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            Dataverse dv = MetadataManager.INSTANCE.getDataverse(mdTxnCtx, dataverse);
            if (dv == null) {
                return false;
            }

            org.apache.asterix.metadata.entities.Library library = MetadataManager.INSTANCE.getLibrary(mdTxnCtx,
                    dataverse, libraryName);
            if (library == null) {
                return false;
            }

            List<org.apache.asterix.metadata.entities.Function> functions = MetadataManager.INSTANCE
                    .getDataverseFunctions(mdTxnCtx, dataverse);
            for (org.apache.asterix.metadata.entities.Function function : functions) {
                if (function.getName().startsWith(libraryName + "#")) {
                    MetadataManager.INSTANCE.dropFunction(mdTxnCtx,
                            new FunctionSignature(dataverse, function.getName(), function.getArity()));
                }
            }

            List<org.apache.asterix.metadata.entities.DatasourceAdapter> adapters = MetadataManager.INSTANCE
                    .getDataverseAdapters(mdTxnCtx, dataverse);
            for (org.apache.asterix.metadata.entities.DatasourceAdapter adapter : adapters) {
                if (adapter.getAdapterIdentifier().getName().startsWith(libraryName + "#")) {
                    MetadataManager.INSTANCE.dropAdapter(mdTxnCtx, dataverse, adapter.getAdapterIdentifier().getName());
                }
            }

            MetadataManager.INSTANCE.dropLibrary(mdTxnCtx, dataverse, libraryName);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            MetadataManager.INSTANCE.abortTransaction(mdTxnCtx);
            throw new AsterixException(e);
        }
        return true;
    }

    // Each element of a library is installed as part of a transaction. Any
    // failure in installing an element does not effect installation of other
    // libraries
    private static void installLibraryIfNeeded(String dataverse, final File libraryDir,
            Map<String, List<String>> uninstalledLibs) throws Exception {

        String libraryName = libraryDir.getName().trim();
        List<String> uninstalledLibsInDv = uninstalledLibs.get(dataverse);
        boolean wasUninstalled = uninstalledLibsInDv != null && uninstalledLibsInDv.contains(libraryName);

        MetadataTransactionContext mdTxnCtx = null;
        MetadataManager.INSTANCE.acquireWriteLatch();
        try {
            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            org.apache.asterix.metadata.entities.Library libraryInMetadata = MetadataManager.INSTANCE
                    .getLibrary(mdTxnCtx, dataverse, libraryName);
            if (libraryInMetadata != null && !wasUninstalled) {
                return;
            }

            String[] libraryDescriptors = libraryDir.list(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    return name.endsWith(".xml");
                }
            });

            ExternalLibrary library = getLibrary(new File(libraryDir + File.separator + libraryDescriptors[0]));

            if (libraryDescriptors.length == 0) {
                throw new Exception("No library descriptor defined");
            } else if (libraryDescriptors.length > 1) {
                throw new Exception("More than 1 library descriptors defined");
            }

            Dataverse dv = MetadataManager.INSTANCE.getDataverse(mdTxnCtx, dataverse);
            if (dv == null) {
                MetadataManager.INSTANCE.addDataverse(mdTxnCtx, new Dataverse(dataverse,
                        NonTaggedDataFormat.NON_TAGGED_DATA_FORMAT, IMetadataEntity.PENDING_NO_OP));
            }
            if (library.getLibraryFunctions() != null) {
                for (LibraryFunction function : library.getLibraryFunctions().getLibraryFunction()) {
                    String[] fargs = function.getArguments().trim().split(",");
                    List<String> args = new ArrayList<String>();
                    for (String arg : fargs) {
                        args.add(arg);
                    }
                    org.apache.asterix.metadata.entities.Function f = new org.apache.asterix.metadata.entities.Function(
                            dataverse, libraryName + "#" + function.getName().trim(), args.size(), args,
                            function.getReturnType().trim(), function.getDefinition().trim(),
                            library.getLanguage().trim(), function.getFunctionType().trim());
                    MetadataManager.INSTANCE.addFunction(mdTxnCtx, f);
                    if (LOGGER.isLoggable(Level.INFO)) {
                        LOGGER.info("Installed function: " + libraryName + "#" + function.getName().trim());
                    }
                }
            }

            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Installed functions contain in library :" + libraryName);
            }

            if (library.getLibraryAdapters() != null) {
                for (LibraryAdapter adapter : library.getLibraryAdapters().getLibraryAdapter()) {
                    String adapterFactoryClass = adapter.getFactoryClass().trim();
                    String adapterName = libraryName + "#" + adapter.getName().trim();
                    AdapterIdentifier aid = new AdapterIdentifier(dataverse, adapterName);
                    DatasourceAdapter dsa = new DatasourceAdapter(aid, adapterFactoryClass, AdapterType.EXTERNAL);
                    MetadataManager.INSTANCE.addAdapter(mdTxnCtx, dsa);
                    if (LOGGER.isLoggable(Level.INFO)) {
                        LOGGER.info("Installed adapter: " + adapterName);
                    }
                }
            }

            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Installed adapters contain in library :" + libraryName);
            }

            MetadataManager.INSTANCE.addLibrary(mdTxnCtx, new Library(dataverse, libraryName));

            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Added library " + libraryName + "to Metadata");
            }

            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            e.printStackTrace();
            if (LOGGER.isLoggable(Level.SEVERE)) {
                LOGGER.info("Exception in installing library " + libraryName);
            }
            MetadataManager.INSTANCE.abortTransaction(mdTxnCtx);
        } finally {
            MetadataManager.INSTANCE.releaseWriteLatch();
        }
    }

    private static void registerLibrary(String dataverse, String libraryName, boolean isMetadataNode,
            File installLibDir) throws Exception {
        ClassLoader classLoader = getLibraryClassLoader(dataverse, libraryName);
        ExternalLibraryManager.registerLibraryClassLoader(dataverse, libraryName, classLoader);
    }

    private static ExternalLibrary getLibrary(File libraryXMLPath) throws Exception {
        JAXBContext configCtx = JAXBContext.newInstance(ExternalLibrary.class);
        Unmarshaller unmarshaller = configCtx.createUnmarshaller();
        ExternalLibrary library = (ExternalLibrary) unmarshaller.unmarshal(libraryXMLPath);
        return library;
    }

    private static ClassLoader getLibraryClassLoader(String dataverse, String libraryName) throws Exception {

        File installDir = getLibraryInstallDir();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Installing lirbary " + libraryName + " in dataverse " + dataverse + "."
                    + " Install Directory: " + installDir.getAbsolutePath());
        }

        File libDir = new File(
                installDir.getAbsolutePath() + File.separator + dataverse + File.separator + libraryName);
        FilenameFilter jarFileFilter = new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".jar");
            }
        };

        String[] jarsInLibDir = libDir.list(jarFileFilter);
        if (jarsInLibDir.length > 1) {
            throw new Exception("Incorrect library structure: found multiple library jars");
        }
        if (jarsInLibDir.length < 0) {
            throw new Exception("Incorrect library structure: could not find library jar");
        }

        File libJar = new File(libDir, jarsInLibDir[0]);
        File libDependencyDir = new File(libDir.getAbsolutePath() + File.separator + "lib");
        int numDependencies = 1;
        String[] libraryDependencies = null;
        if (libDependencyDir.exists()) {
            libraryDependencies = libDependencyDir.list(jarFileFilter);
            numDependencies += libraryDependencies.length;
        }

        ClassLoader parentClassLoader = ExternalLibraryBootstrap.class.getClassLoader();
        URL[] urls = new URL[numDependencies];
        int count = 0;
        urls[count++] = libJar.toURI().toURL();

        if (libraryDependencies != null && libraryDependencies.length > 0) {
            for (String dependency : libraryDependencies) {
                File file = new File(libDependencyDir + File.separator + dependency);
                urls[count++] = file.toURI().toURL();
            }
        }

        if (LOGGER.isLoggable(Level.INFO)) {
            StringBuilder logMesg = new StringBuilder("Classpath for library " + libraryName + "\n");
            for (URL url : urls) {
                logMesg.append(url.getFile() + "\n");
            }
            LOGGER.info(logMesg.toString());
        }

        ClassLoader classLoader = new URLClassLoader(urls, parentClassLoader);
        return classLoader;
    }

    private static File getLibraryInstallDir() {
        String workingDir = System.getProperty("user.dir");
        return new File(workingDir + File.separator + "library");
    }

    private static File getLibraryUninstallDir() {
        String workingDir = System.getProperty("user.dir");
        return new File(workingDir + File.separator + "uninstall");
    }

}
