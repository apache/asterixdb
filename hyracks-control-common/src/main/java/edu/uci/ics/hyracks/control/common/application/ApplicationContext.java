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
package edu.uci.ics.hyracks.control.common.application;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import edu.uci.ics.hyracks.api.application.IApplicationContext;
import edu.uci.ics.hyracks.api.application.IBootstrap;
import edu.uci.ics.hyracks.api.util.JavaSerializationUtils;
import edu.uci.ics.hyracks.control.common.context.ServerContext;

public abstract class ApplicationContext implements IApplicationContext {
    private static final String APPLICATION_ROOT = "applications";
    private static final String CLUSTER_CONTROLLER_BOOTSTRAP_CLASS_KEY = "cc.bootstrap.class";
    private static final String NODE_CONTROLLER_BOOTSTRAP_CLASS_KEY = "nc.bootstrap.class";

    protected ServerContext serverCtx;
    protected final String appName;
    protected final File applicationRootDir;
    protected ClassLoader classLoader;
    protected ApplicationStatus status;
    protected Properties deploymentDescriptor;
    protected IBootstrap bootstrap;
    protected Serializable distributedState;

    public ApplicationContext(ServerContext serverCtx, String appName) throws IOException {
        this.serverCtx = serverCtx;
        this.appName = appName;
        this.applicationRootDir = new File(new File(serverCtx.getBaseDir(), APPLICATION_ROOT), appName);
        status = ApplicationStatus.CREATED;
        FileUtils.deleteDirectory(applicationRootDir);
        applicationRootDir.mkdirs();
    }

    public String getApplicationName() {
        return appName;
    }

    public void initializeClassPath() throws Exception {
        if (expandArchive()) {
            File expandedFolder = getExpandedFolder();
            List<URL> urls = new ArrayList<URL>();
            findJarFiles(expandedFolder, urls);
            Collections.sort(urls, new Comparator<URL>() {
                @Override
                public int compare(URL o1, URL o2) {
                    return o1.getFile().compareTo(o2.getFile());
                }
            });
            classLoader = new URLClassLoader(urls.toArray(new URL[urls.size()]));
        } else {
            classLoader = getClass().getClassLoader();
        }
        deploymentDescriptor = parseDeploymentDescriptor();
    }

    public void initialize() throws Exception {
        if (status != ApplicationStatus.CREATED) {
            throw new IllegalStateException();
        }
        if (deploymentDescriptor != null) {
            String bootstrapClass = null;
            switch (serverCtx.getServerType()) {
                case CLUSTER_CONTROLLER: {
                    bootstrapClass = deploymentDescriptor.getProperty(CLUSTER_CONTROLLER_BOOTSTRAP_CLASS_KEY);
                    break;
                }
                case NODE_CONTROLLER: {
                    bootstrapClass = deploymentDescriptor.getProperty(NODE_CONTROLLER_BOOTSTRAP_CLASS_KEY);
                    break;
                }
            }
            if (bootstrapClass != null) {
                bootstrap = (IBootstrap) classLoader.loadClass(bootstrapClass).newInstance();
                start();
            }
        }
        status = ApplicationStatus.INITIALIZED;
    }

    protected abstract void start() throws Exception;

    protected abstract void stop() throws Exception;

    private void findJarFiles(File dir, List<URL> urls) throws MalformedURLException {
        for (File f : dir.listFiles()) {
            if (f.isDirectory()) {
                findJarFiles(f, urls);
            } else if (f.getName().endsWith(".jar") || f.getName().endsWith(".zip")) {
                urls.add(f.toURI().toURL());
            }
        }
    }

    private Properties parseDeploymentDescriptor() throws IOException {
        InputStream in = classLoader.getResourceAsStream("hyracks-deployment.properties");
        Properties props = new Properties();
        if (in != null) {
            try {
                props.load(in);
            } finally {
                in.close();
            }
        }
        return props;
    }

    private boolean expandArchive() throws IOException {
        File archiveFile = getArchiveFile();
        if (archiveFile.exists()) {
            File expandedFolder = getExpandedFolder();
            FileUtils.deleteDirectory(expandedFolder);
            ZipFile zf = new ZipFile(archiveFile);
            for (Enumeration<? extends ZipEntry> i = zf.entries(); i.hasMoreElements();) {
                ZipEntry ze = i.nextElement();
                String name = ze.getName();
                if (name.endsWith("/")) {
                    continue;
                }
                InputStream is = zf.getInputStream(ze);
                OutputStream os = FileUtils.openOutputStream(new File(expandedFolder, name));
                try {
                    IOUtils.copyLarge(is, os);
                } finally {
                    os.close();
                    is.close();
                }
            }
            return true;
        }
        return false;
    }

    private File getExpandedFolder() {
        return new File(applicationRootDir, "expanded");
    }

    public void deinitialize() throws Exception {
        status = ApplicationStatus.DEINITIALIZED;
        stop();
        File expandedFolder = getExpandedFolder();
        FileUtils.deleteDirectory(expandedFolder);
    }

    public Object deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
        return JavaSerializationUtils.deserialize(bytes, classLoader);
    }

    public OutputStream getHarOutputStream() throws IOException {
        return new FileOutputStream(getArchiveFile());
    }

    private File getArchiveFile() {
        return new File(applicationRootDir, "application.har");
    }

    public InputStream getHarInputStream() throws IOException {
        return new FileInputStream(getArchiveFile());
    }

    public boolean containsHar() {
        return getArchiveFile().exists();
    }

    @Override
    public Serializable getDistributedState() {
        return distributedState;
    }

    @Override
    public ClassLoader getClassLoader() {
        return classLoader;
    }
}
