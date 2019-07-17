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
package org.apache.asterix.api.http.server;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import org.apache.asterix.app.external.ExternalLibraryUtils;
import org.apache.asterix.app.message.DeleteUdfMessage;
import org.apache.asterix.app.message.LoadUdfMessage;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.messaging.api.ICCMessageBroker;
import org.apache.asterix.common.messaging.api.INcAddressedMessage;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.deployment.DeploymentId;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.AbstractServlet;
import org.apache.hyracks.util.file.FileUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.QueryStringDecoder;

public class UdfApiServlet extends AbstractServlet {

    private static final Logger LOGGER = LogManager.getLogger();
    private final ICcApplicationContext appCtx;
    private final ICCMessageBroker broker;
    public static final String UDF_TMP_DIR_PREFIX = "udf_temp";
    public static final int UDF_RESPONSE_TIMEOUT = 5000;

    public UdfApiServlet(ICcApplicationContext appCtx, ConcurrentMap<String, Object> ctx, String... paths) {
        super(ctx, paths);
        this.appCtx = appCtx;
        this.broker = (ICCMessageBroker) appCtx.getServiceContext().getMessageBroker();
    }

    private String[] getResource(FullHttpRequest req) throws IllegalArgumentException {
        String[] path = new QueryStringDecoder(req.uri()).path().split("/");
        if (path.length != 5) {
            throw new IllegalArgumentException("Invalid resource.");
        }
        String resourceName = path[path.length - 1];
        String dataverseName = path[path.length - 2];
        return new String[] { resourceName, dataverseName };
    }

    @Override
    protected void post(IServletRequest request, IServletResponse response) {
        FullHttpRequest req = request.getHttpRequest();
        String[] resourceNames;
        try {
            resourceNames = getResource(req);
        } catch (IllegalArgumentException e) {
            response.setStatus(HttpResponseStatus.BAD_REQUEST);
            return;
        }
        String resourceName = resourceNames[0];
        String dataverse = resourceNames[1];
        File udf = null;
        try {
            File workingDir = new File(appCtx.getServiceContext().getServerCtx().getBaseDir().getAbsolutePath(),
                    UDF_TMP_DIR_PREFIX);
            if (!workingDir.exists()) {
                FileUtil.forceMkdirs(workingDir);
            }
            udf = File.createTempFile(resourceName, ".zip", workingDir);
            try (RandomAccessFile raf = new RandomAccessFile(udf, "rw")) {
                ByteBuf reqContent = req.content();
                raf.setLength(reqContent.readableBytes());
                FileChannel fc = raf.getChannel();
                ByteBuffer content = reqContent.nioBuffer();
                while (content.hasRemaining()) {
                    fc.write(content);
                }
            }
            IHyracksClientConnection hcc = appCtx.getHcc();
            DeploymentId udfName = new DeploymentId(dataverse + "." + resourceName);
            ClassLoader cl = appCtx.getLibraryManager().getLibraryClassLoader(dataverse, resourceName);
            if (cl != null) {
                deleteUdf(dataverse, resourceName);
            }
            hcc.deployBinary(udfName, Arrays.asList(udf.toString()), true);
            ExternalLibraryUtils.setUpExternaLibrary(appCtx.getLibraryManager(), false,
                    FileUtil.joinPath(appCtx.getServiceContext().getServerCtx().getBaseDir().getAbsolutePath(),
                            "applications", udfName.toString()));

            long reqId = broker.newRequestId();
            List<INcAddressedMessage> requests = new ArrayList<>();
            List<String> ncs = new ArrayList<>(appCtx.getClusterStateManager().getParticipantNodes());
            ncs.forEach(s -> requests.add(new LoadUdfMessage(dataverse, resourceName, reqId)));
            broker.sendSyncRequestToNCs(reqId, ncs, requests, UDF_RESPONSE_TIMEOUT);
        } catch (Exception e) {
            response.setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
            LOGGER.error(e);
            return;
        } finally {
            if (udf != null) {
                udf.delete();
            }
        }
        response.setStatus(HttpResponseStatus.OK);

    }

    private void deleteUdf(String dataverse, String resourceName) throws Exception {
        long reqId = broker.newRequestId();
        List<INcAddressedMessage> requests = new ArrayList<>();
        List<String> ncs = new ArrayList<>(appCtx.getClusterStateManager().getParticipantNodes());
        ncs.forEach(s -> requests.add(new DeleteUdfMessage(dataverse, resourceName, reqId)));
        broker.sendSyncRequestToNCs(reqId, ncs, requests, UDF_RESPONSE_TIMEOUT);
        appCtx.getLibraryManager().deregisterLibraryClassLoader(dataverse, resourceName);
        appCtx.getHcc().unDeployBinary(new DeploymentId(resourceName));
    }

    @Override
    protected void delete(IServletRequest request, IServletResponse response) {
        String[] resourceNames;
        try {
            resourceNames = getResource(request.getHttpRequest());
        } catch (IllegalArgumentException e) {
            response.setStatus(HttpResponseStatus.BAD_REQUEST);
            return;
        }
        String resourceName = resourceNames[0];
        String dataverse = resourceNames[1];
        try {
            deleteUdf(dataverse, resourceName);
        } catch (Exception e) {
            response.setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
            LOGGER.error(e);
            return;
        }
        response.setStatus(HttpResponseStatus.OK);
    }
}
