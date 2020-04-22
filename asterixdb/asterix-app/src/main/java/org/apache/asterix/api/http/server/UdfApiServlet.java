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
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import org.apache.asterix.app.external.ExternalLibraryUtils;
import org.apache.asterix.app.message.LoadUdfMessage;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.messaging.api.ICCMessageBroker;
import org.apache.asterix.common.messaging.api.INcAddressedMessage;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.IMetadataLockUtil;
import org.apache.asterix.common.metadata.LockList;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.DatasourceAdapter;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.asterix.metadata.entities.Function;
import org.apache.asterix.metadata.entities.Library;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.deployment.DeploymentId;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.util.file.FileUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.QueryStringDecoder;

public class UdfApiServlet extends BasicAuthServlet {

    private static final Logger LOGGER = LogManager.getLogger();
    private final ICcApplicationContext appCtx;
    private final ICCMessageBroker broker;
    public static final String UDF_TMP_DIR_PREFIX = "udf_temp";
    public static final int UDF_RESPONSE_TIMEOUT = 5000;
    public static final int URL_PREFIX_LENGTH = 3;

    public UdfApiServlet(ICcApplicationContext appCtx, ConcurrentMap<String, Object> ctx, String... paths) {
        super(ctx, paths);
        this.appCtx = appCtx;
        this.broker = (ICCMessageBroker) appCtx.getServiceContext().getMessageBroker();
    }

    private Pair<String, DataverseName> getResource(FullHttpRequest req) throws IllegalArgumentException {
        String[] path = new QueryStringDecoder(req.uri()).path().split("/");
        if (path.length != 5) {
            throw new IllegalArgumentException("Invalid resource.");
        }
        String resourceName = path[path.length - 1];
        DataverseName dataverseName = DataverseName.createFromCanonicalForm(path[path.length - 2]); // TODO: use path separators instead for multiparts
        return new Pair<>(resourceName, dataverseName);
    }

    @Override
    protected void post(IServletRequest request, IServletResponse response) {

        PrintWriter responseWriter = response.writer();
        FullHttpRequest req = request.getHttpRequest();
        Pair<String, DataverseName> resourceNames;
        try {
            resourceNames = getResource(req);
        } catch (IllegalArgumentException e) {
            response.setStatus(HttpResponseStatus.BAD_REQUEST);
            return;
        }
        String resourceName = resourceNames.first;
        DataverseName dataverse = resourceNames.second;
        IMetadataLockUtil mdLockUtil = appCtx.getMetadataLockUtil();
        MetadataTransactionContext mdTxnCtx = null;
        LockList mdLockList = null;
        File udf = null;
        try {
            MetadataManager.INSTANCE.init();
            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            MetadataProvider metadataProvider = MetadataProvider.create(appCtx, null);
            mdLockList = metadataProvider.getLocks();
            mdLockUtil.createLibraryBegin(appCtx.getMetadataLockManager(), metadataProvider.getLocks(), dataverse,
                    resourceName);
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
            setupBinariesAndClassloaders(dataverse, resourceName, udf);
            installLibrary(mdTxnCtx, dataverse, resourceName);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            try {
                ExternalLibraryUtils.deleteDeployedUdf(broker, appCtx, dataverse, resourceName);
            } catch (Exception e2) {
                e.addSuppressed(e2);
            }
            response.setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
            responseWriter.write(e.getMessage());
            responseWriter.flush();
            LOGGER.error(e);
            if (mdTxnCtx != null) {
                try {
                    MetadataManager.INSTANCE.abortTransaction(mdTxnCtx);
                } catch (RemoteException r) {
                    LOGGER.error("Unable to abort metadata transaction", r);
                }
            }
            if (udf != null) {
                udf.delete();
            }
            return;
        } finally {
            if (mdLockList != null) {
                mdLockList.unlock();
            }
        }
        response.setStatus(HttpResponseStatus.OK);

    }

    private void setupBinariesAndClassloaders(DataverseName dataverse, String resourceName, File udf) throws Exception {
        IHyracksClientConnection hcc = appCtx.getHcc();
        DeploymentId udfName = new DeploymentId(makeDeploymentId(dataverse, resourceName));
        ClassLoader cl = appCtx.getLibraryManager().getLibraryClassLoader(dataverse, resourceName);
        if (cl != null) {
            //prepare to replace the binary
            ExternalLibraryUtils.deleteDeployedUdf(broker, appCtx, dataverse, resourceName);
        }
        hcc.deployBinary(udfName, Arrays.asList(udf.toString()), true);
        //setup for CC
        ExternalLibraryUtils.setUpExternaLibrary(appCtx.getLibraryManager(),
                FileUtil.joinPath(appCtx.getServiceContext().getServerCtx().getBaseDir().getAbsolutePath(),
                        "applications", udfName.toString()));
        //setup NCs
        long reqId = broker.newRequestId();
        List<INcAddressedMessage> requests = new ArrayList<>();
        List<String> ncs = new ArrayList<>(appCtx.getClusterStateManager().getParticipantNodes());
        ncs.forEach(s -> requests.add(new LoadUdfMessage(dataverse, resourceName, reqId)));
        broker.sendSyncRequestToNCs(reqId, ncs, requests, UDF_RESPONSE_TIMEOUT);
    }

    private static void installLibrary(MetadataTransactionContext mdTxnCtx, DataverseName dataverse, String libraryName)
            throws RemoteException, AlgebricksException {
        Library libraryInMetadata = MetadataManager.INSTANCE.getLibrary(mdTxnCtx, dataverse, libraryName);
        // Get the dataverse
        Dataverse dv = MetadataManager.INSTANCE.getDataverse(mdTxnCtx, dataverse);
        if (dv == null) {
            throw new AsterixException(ErrorCode.UNKNOWN_DATAVERSE);
        }
        if (libraryInMetadata != null) {
            //replacing binary, library already exists
            return;
        }
        // Add library
        MetadataManager.INSTANCE.addLibrary(mdTxnCtx, new Library(dataverse, libraryName));
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Added library " + libraryName + " to Metadata");
        }
    }

    private static void deleteLibrary(MetadataTransactionContext mdTxnCtx, DataverseName dataverse, String libraryName)
            throws RemoteException, AlgebricksException {
        Dataverse dv = MetadataManager.INSTANCE.getDataverse(mdTxnCtx, dataverse);
        if (dv == null) {
            throw new AsterixException(ErrorCode.UNKNOWN_DATAVERSE);
        }
        Library library = MetadataManager.INSTANCE.getLibrary(mdTxnCtx, dataverse, libraryName);
        if (library == null) {
            throw new AsterixException(ErrorCode.UNKNOWN_LIBRARY);
        }
        List<Function> functions = MetadataManager.INSTANCE.getDataverseFunctions(mdTxnCtx, dataverse);
        for (Function function : functions) {
            if (libraryName.equals(function.getLibrary())) {
                throw new AsterixException(ErrorCode.METADATA_DROP_LIBRARY_IN_USE, libraryName);
            }
        }
        List<DatasourceAdapter> adapters = MetadataManager.INSTANCE.getDataverseAdapters(mdTxnCtx, dataverse);
        for (DatasourceAdapter adapter : adapters) {
            if (libraryName.equals(adapter.getLibrary())) {
                throw new AsterixException(ErrorCode.METADATA_DROP_LIBRARY_IN_USE, libraryName);
            }
        }
        MetadataManager.INSTANCE.dropLibrary(mdTxnCtx, dataverse, libraryName);
    }

    public static String makeDeploymentId(DataverseName dv, String resourceName) {
        List<String> dvParts = dv.getParts();
        dvParts.add(resourceName);
        DataverseName dvWithLibrarySuffix = DataverseName.create(dvParts);
        return dvWithLibrarySuffix.getCanonicalForm();
    }

    @Override
    protected void delete(IServletRequest request, IServletResponse response) {
        Pair<String, DataverseName> resourceNames;
        try {
            resourceNames = getResource(request.getHttpRequest());
        } catch (IllegalArgumentException e) {
            response.setStatus(HttpResponseStatus.BAD_REQUEST);
            return;
        }
        PrintWriter responseWriter = response.writer();
        String resourceName = resourceNames.first;
        DataverseName dataverse = resourceNames.second;
        IMetadataLockUtil mdLockUtil = appCtx.getMetadataLockUtil();
        MetadataTransactionContext mdTxnCtx = null;
        LockList mdLockList = null;
        try {
            MetadataManager.INSTANCE.init();
            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            MetadataProvider metadataProvider = MetadataProvider.create(appCtx, null);
            mdLockList = metadataProvider.getLocks();
            mdLockUtil.dropLibraryBegin(appCtx.getMetadataLockManager(), metadataProvider.getLocks(), dataverse,
                    resourceName);
            deleteLibrary(mdTxnCtx, dataverse, resourceName);
            ExternalLibraryUtils.deleteDeployedUdf(broker, appCtx, dataverse, resourceName);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            try {
                MetadataManager.INSTANCE.abortTransaction(mdTxnCtx);
            } catch (RemoteException r) {
                LOGGER.error("Unable to abort metadata transaction", r);
            }
            response.setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
            responseWriter.write(e.getMessage());
            responseWriter.flush();
            LOGGER.error(e);
            return;
        } finally {
            if (mdLockList != null) {
                mdLockList.unlock();
            }
        }
        response.setStatus(HttpResponseStatus.OK);
    }
}
