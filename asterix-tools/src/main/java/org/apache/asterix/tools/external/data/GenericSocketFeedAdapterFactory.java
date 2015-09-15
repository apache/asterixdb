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
package org.apache.asterix.tools.external.data;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import org.apache.asterix.common.feeds.api.IDatasourceAdapter;
import org.apache.asterix.common.feeds.api.IIntakeProgressTracker;
import org.apache.asterix.external.adapter.factory.StreamBasedAdapterFactory;
import org.apache.asterix.metadata.feeds.IFeedAdapterFactory;
import org.apache.asterix.metadata.entities.ExternalFile;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.util.AsterixRuntimeUtil;
import org.apache.asterix.runtime.operators.file.AsterixTupleParserFactory.InputDataFormat;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.context.IHyracksTaskContext;

/**
 * Factory class for creating @see{GenericSocketFeedAdapter} The
 * adapter listens at a port for receiving data (from external world).
 * Data received is transformed into Asterix Data Format (ADM).
 */
public class GenericSocketFeedAdapterFactory extends StreamBasedAdapterFactory implements IFeedAdapterFactory {

    private static final long serialVersionUID = 1L;

    private ARecordType outputType;

    private List<Pair<String, Integer>> sockets;

    private Mode mode = Mode.IP;

    public static final String KEY_SOCKETS = "sockets";

    public static final String KEY_MODE = "address-type";

    public static enum Mode {
        NC,
        IP
    }

    @Override
    public String getName() {
        return "socket_adapter";
    }

    @Override
    public SupportedOperation getSupportedOperations() {
        return SupportedOperation.READ;
    }

    public List<Pair<String, Integer>> getSockets() {
        return sockets;
    }

    @Override
    public void configure(Map<String, String> configuration, ARecordType outputType) throws Exception {
        this.configuration = configuration;
        this.configureSockets(configuration);
        this.configureFormat(outputType);
        this.outputType = (ARecordType) outputType;
    }

    @Override
    public AlgebricksPartitionConstraint getPartitionConstraint() throws Exception {
        List<String> locations = new ArrayList<String>();
        for (Pair<String, Integer> socket : sockets) {
            locations.add(socket.first);
        }
        return new AlgebricksAbsolutePartitionConstraint(locations.toArray(new String[] {}));
    }

    @Override
    public IDatasourceAdapter createAdapter(IHyracksTaskContext ctx, int partition) throws Exception {
        Pair<String, Integer> socket = sockets.get(partition);
        return new GenericSocketFeedAdapter(parserFactory, outputType, socket.second, ctx, partition);
    }

    private void configureSockets(Map<String, String> configuration) throws Exception {
        sockets = new ArrayList<Pair<String, Integer>>();
        String modeValue = configuration.get(KEY_MODE);
        if (modeValue != null) {
            mode = Mode.valueOf(modeValue.trim().toUpperCase());
        }
        String socketsValue = configuration.get(KEY_SOCKETS);
        if (socketsValue == null) {
            throw new IllegalArgumentException("\'sockets\' parameter not specified as part of adapter configuration");
        }
        Map<InetAddress, Set<String>> ncMap = AsterixRuntimeUtil.getNodeControllerMap();
        List<String> ncs = AsterixRuntimeUtil.getAllNodeControllers();
        String[] socketsArray = socketsValue.split(",");
        Random random = new Random();
        for (String socket : socketsArray) {
            String[] socketTokens = socket.split(":");
            String host = socketTokens[0].trim();
            int port = Integer.parseInt(socketTokens[1].trim());
            Pair<String, Integer> p = null;
            switch (mode) {
                case IP:
                    Set<String> ncsOnIp = ncMap.get(InetAddress.getByName(host));
                    if (ncsOnIp == null || ncsOnIp.isEmpty()) {
                        throw new IllegalArgumentException("Invalid host " + host
                                + " as it is not part of the AsterixDB cluster. Valid choices are "
                                + StringUtils.join(ncMap.keySet(), ", "));
                    }
                    String[] ncArray = ncsOnIp.toArray(new String[] {});
                    String nc = ncArray[random.nextInt(ncArray.length)];
                    p = new Pair<String, Integer>(nc, port);
                    break;

                case NC:
                    p = new Pair<String, Integer>(host, port);
                    if (!ncs.contains(host)) {
                        throw new IllegalArgumentException("Invalid NC " + host
                                + " as it is not part of the AsterixDB cluster. Valid choices are "
                                + StringUtils.join(ncs, ", "));

                    }
                    break;
            }
            sockets.add(p);
        }
    }

    @Override
    public ARecordType getAdapterOutputType() {
        return outputType;
    }

    @Override
    public InputDataFormat getInputDataFormat() {
        return InputDataFormat.UNKNOWN;
    }

    public boolean isRecordTrackingEnabled() {
        return false;
    }

    public IIntakeProgressTracker createIntakeProgressTracker() {
        throw new UnsupportedOperationException("Tracking of ingested records not enabled");
    }
    
    public void setFiles(List<ExternalFile> files) throws AlgebricksException {
        throw new AlgebricksException("files access not supported for this adapter");
    }
}