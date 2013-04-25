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

package edu.uci.ics.hyracks.control.common.deployment;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;

import edu.uci.ics.hyracks.api.application.IApplicationContext;
import edu.uci.ics.hyracks.api.deployment.DeploymentId;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.IJobSerializerDeserializer;
import edu.uci.ics.hyracks.api.job.IJobSerializerDeserializerContainer;
import edu.uci.ics.hyracks.api.util.JavaSerializationUtils;
import edu.uci.ics.hyracks.control.common.context.ServerContext;

/**
 * A utility class which is in charge of the actual work of deployments.
 * 
 * @author yingyib
 */
public class DeploymentUtils {

    private static final String DEPLOYMENT = "applications";

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
            throw new HyracksException(e);
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
     *            the ServerContext
     * @param isNC
     *            true is NC/false is CC
     * @throws HyracksException
     */
    public static void deploy(DeploymentId deploymentId, List<URL> urls, IJobSerializerDeserializerContainer container,
            ServerContext ctx, boolean isNC) throws HyracksException {
        IJobSerializerDeserializer jobSerDe = container.getJobSerializerDeerializer(deploymentId);
        if (jobSerDe == null) {
            jobSerDe = new ClassLoaderJobSerializerDeserializer();
            container.addJobSerializerDeserializer(deploymentId, jobSerDe);
        }
        String rootDir = ctx.getBaseDir().toString();
        String deploymentDir = rootDir.endsWith(File.separator) ? rootDir + DEPLOYMENT + File.separator + deploymentId
                : rootDir + File.separator + DEPLOYMENT + File.separator + deploymentId;
        jobSerDe.addClassPathURLs(downloadURLs(urls, deploymentDir, isNC));
    }

    /**
     * Deserialize bytes to an object according to a specific deployment
     * 
     * @param bytes
     *            the bytes to be deserialized
     * @param deploymentId
     *            the deployment id
     * @param appCtx
     * @return the deserialized object
     * @throws HyracksException
     */
    public static Object deserialize(byte[] bytes, DeploymentId deploymentId, IApplicationContext appCtx)
            throws HyracksException {
        try {
            IJobSerializerDeserializerContainer jobSerDeContainer = appCtx.getJobSerializerDeserializerContainer();
            IJobSerializerDeserializer jobSerDe = deploymentId == null ? null : jobSerDeContainer
                    .getJobSerializerDeerializer(deploymentId);
            Object obj = jobSerDe == null ? JavaSerializationUtils.deserialize(bytes) : jobSerDe.deserialize(bytes);
            return obj;
        } catch (Exception e) {
            throw new HyracksException(e);
        }
    }

    /**
     * Load a class from its class name
     * 
     * @param className
     * @param deploymentId
     * @param appCtx
     * @return the loaded class
     * @throws HyracksException
     */
    public static Class<?> loadClass(String className, DeploymentId deploymentId, IApplicationContext appCtx)
            throws HyracksException {
        try {
            IJobSerializerDeserializerContainer jobSerDeContainer = appCtx.getJobSerializerDeserializerContainer();
            IJobSerializerDeserializer jobSerDe = deploymentId == null ? null : jobSerDeContainer
                    .getJobSerializerDeerializer(deploymentId);
            Class<?> cl = jobSerDe == null ? JavaSerializationUtils.loadClass(className) : jobSerDe
                    .loadClass(className);
            return cl;
        } catch (Exception e) {
            throw new HyracksException(e);
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
    public static ClassLoader getClassLoader(DeploymentId deploymentId, IApplicationContext appCtx)
            throws HyracksException {
        try {
            IJobSerializerDeserializerContainer jobSerDeContainer = appCtx.getJobSerializerDeserializerContainer();
            IJobSerializerDeserializer jobSerDe = deploymentId == null ? null : jobSerDeContainer
                    .getJobSerializerDeerializer(deploymentId);
            ClassLoader cl = jobSerDe == null ? DeploymentUtils.class.getClassLoader() : jobSerDe.getClassLoader();
            return cl;
        } catch (Exception e) {
            throw new HyracksException(e);
        }
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
    private static List<URL> downloadURLs(List<URL> urls, String deploymentDir, boolean isNC) throws HyracksException {
        try {
            List<URL> downloadedFileURLs = new ArrayList<URL>();
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
                    HttpClient hc = new DefaultHttpClient();
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
                downloadedFileURLs.add(targetFile.toURI().toURL());
            }
            return downloadedFileURLs;
        } catch (Exception e) {
            throw new HyracksException(e);
        }
    }
}
