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

package org.apache.hyracks.control.common.deployment;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.hyracks.api.application.IServiceContext;
import org.apache.hyracks.api.deployment.DeploymentId;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.IJobSerializerDeserializer;
import org.apache.hyracks.api.job.IJobSerializerDeserializerContainer;
import org.apache.hyracks.api.util.JavaSerializationUtils;
import org.apache.hyracks.control.common.context.ServerContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A utility class which is in charge of the actual work of deployments.
 *
 * @author yingyib
 */
public class DeploymentUtils {

    private static final String DEPLOYMENT = "applications";
    private static final Logger LOGGER = LogManager.getLogger();

    /**
     * undeploy an existing deployment
     *
     * @param deploymentId
     *            the deployment id
     * @param container
     * @param ctx
     * @throws HyracksException
     */
    public static void undeploy(DeploymentId deploymentId, IJobSerializerDeserializerContainer container,
            ServerContext ctx) throws HyracksException {
        container.removeJobSerializerDeserializer(deploymentId);
        String rootDir = ctx.getBaseDir().toString();
        String deploymentDir = rootDir.endsWith(File.separator) ? rootDir + DEPLOYMENT + File.separator + deploymentId
                : rootDir + File.separator + DEPLOYMENT + File.separator + deploymentId;
        try {
            File dFile = new File(deploymentDir);
            if (dFile.exists()) {
                FileUtils.forceDelete(dFile);
            }
        } catch (Exception e) {
            throw HyracksException.create(e);
        }
    }

    /**
     * Deploying jars in NC or CC
     *
     * @param deploymentId
     *            the deployment id
     * @param urls
     *            the jar URLs
     * @param container
     *            the container of serailizer/deserializer
     * @param ctx
     *            the ServerContext * @param isNC
     *            true is NC/false is CC
     * @throws HyracksException
     */
    public static void deploy(DeploymentId deploymentId, List<URL> urls, IJobSerializerDeserializerContainer container,
            ServerContext ctx, boolean isNC, boolean extractFromArchive) throws HyracksException {
        IJobSerializerDeserializer jobSerDe = container.getJobSerializerDeserializer(deploymentId);
        if (jobSerDe == null) {
            jobSerDe = new ClassLoaderJobSerializerDeserializer();
            container.addJobSerializerDeserializer(deploymentId, jobSerDe);
        }
        String rootDir = ctx.getBaseDir().toString();
        String deploymentDir = rootDir.endsWith(File.separator) ? rootDir + DEPLOYMENT + File.separator + deploymentId
                : rootDir + File.separator + DEPLOYMENT + File.separator + deploymentId;
        if (extractFromArchive) {
            downloadURLs(urls, deploymentDir, isNC, true);
        } else {
            jobSerDe.addClassPathURLs(downloadURLs(urls, deploymentDir, isNC, false));
        }
    }

    /**
     * Deserialize bytes to an object according to a specific deployment
     *
     * @param bytes
     *            the bytes to be deserialized
     * @param deploymentId
     *            the deployment id
     * @param serviceCtx
     * @return the deserialized object
     * @throws HyracksException
     */
    public static Object deserialize(byte[] bytes, DeploymentId deploymentId, IServiceContext serviceCtx)
            throws HyracksException {
        try {
            IJobSerializerDeserializerContainer jobSerDeContainer = serviceCtx.getJobSerializerDeserializerContainer();
            IJobSerializerDeserializer jobSerDe =
                    deploymentId == null ? null : jobSerDeContainer.getJobSerializerDeserializer(deploymentId);
            return jobSerDe == null ? JavaSerializationUtils.deserialize(bytes) : jobSerDe.deserialize(bytes);
        } catch (Exception e) {
            throw HyracksException.create(e);
        }
    }

    /**
     * Load a class from its class name
     *
     * @param className
     * @param deploymentId
     * @param serviceCtx
     * @return the loaded class
     * @throws HyracksException
     */
    public static Class<?> loadClass(String className, DeploymentId deploymentId, IServiceContext serviceCtx)
            throws HyracksException {
        try {
            IJobSerializerDeserializerContainer jobSerDeContainer = serviceCtx.getJobSerializerDeserializerContainer();
            IJobSerializerDeserializer jobSerDe =
                    deploymentId == null ? null : jobSerDeContainer.getJobSerializerDeserializer(deploymentId);
            return jobSerDe == null ? JavaSerializationUtils.loadClass(className) : jobSerDe.loadClass(className);
        } catch (ClassNotFoundException | IOException e) {
            throw HyracksException.create(e);
        }
    }

    /**
     * Get the classloader of a specific deployment
     *
     * @param deploymentId
     * @param appCtx
     * @return
     * @throws HyracksException
     */
    public static ClassLoader getClassLoader(DeploymentId deploymentId, IServiceContext appCtx)
            throws HyracksException {
        IJobSerializerDeserializerContainer jobSerDeContainer = appCtx.getJobSerializerDeserializerContainer();
        IJobSerializerDeserializer jobSerDe =
                deploymentId == null ? null : jobSerDeContainer.getJobSerializerDeserializer(deploymentId);
        return jobSerDe == null ? DeploymentUtils.class.getClassLoader() : jobSerDe.getClassLoader();
    }

    /**
     * Download remote Http URLs and return the stored local file URLs
     *
     * @param urls
     *            the remote Http URLs
     * @param deploymentDir
     *            the deployment jar storage directory
     * @param isNC
     *            true is NC/false is CC
     * @return a list of local file URLs
     * @throws HyracksException
     */
    private static List<URL> downloadURLs(List<URL> urls, String deploymentDir, boolean isNC,
            boolean extractFromArchive) throws HyracksException {
        //retry 10 times at maximum for downloading binaries
        int retryCount = 10;
        int tried = 0;
        Exception trace = null;
        while (tried < retryCount) {
            try {
                tried++;
                List<URL> downloadedFileURLs = new ArrayList<>();
                File dir = new File(deploymentDir);
                if (!dir.exists()) {
                    FileUtils.forceMkdir(dir);
                }
                for (URL url : urls) {
                    String urlString = url.toString();
                    int slashIndex = urlString.lastIndexOf('/');
                    String fileName = urlString.substring(slashIndex + 1).split("&")[1];
                    String filePath = deploymentDir + File.separator + fileName;
                    File targetFile = new File(filePath);
                    if (isNC) {
                        HttpClient hc = HttpClientBuilder.create().build();
                        HttpGet get = new HttpGet(url.toString());
                        HttpResponse response = hc.execute(get);
                        InputStream is = response.getEntity().getContent();
                        OutputStream os = new FileOutputStream(targetFile);
                        try {
                            IOUtils.copyLarge(is, os);
                        } finally {
                            os.close();
                            is.close();
                        }
                    }
                    if (extractFromArchive) {
                        unzip(targetFile.getAbsolutePath(), deploymentDir);
                    }
                    downloadedFileURLs.add(targetFile.toURI().toURL());
                }
                return downloadedFileURLs;
            } catch (Exception e) {
                LOGGER.error("Unable to fetch binaries from URL", e);
                trace = e;
            }
        }
        throw HyracksException.create(trace);
    }

    public static void unzip(String sourceFile, String outputDir) throws IOException {
        try (ZipFile zipFile = new ZipFile(sourceFile)) {
            Enumeration<? extends ZipEntry> entries = zipFile.entries();
            List<File> createdFiles = new ArrayList<>();
            while (entries.hasMoreElements()) {
                ZipEntry entry = entries.nextElement();
                File entryDestination = new File(outputDir, entry.getName());
                if (!entry.isDirectory()) {
                    entryDestination.getParentFile().mkdirs();
                    try (InputStream in = zipFile.getInputStream(entry);
                            OutputStream out = new FileOutputStream(entryDestination)) {
                        createdFiles.add(entryDestination);
                        IOUtils.copy(in, out);
                    } catch (IOException e) {
                        for (File f : createdFiles) {
                            if (!f.delete()) {
                                LOGGER.error("Couldn't clean up file after failed archive extraction: "
                                        + f.getAbsolutePath());
                            }
                        }
                        throw e;
                    }
                }
            }
        }
    }
}
