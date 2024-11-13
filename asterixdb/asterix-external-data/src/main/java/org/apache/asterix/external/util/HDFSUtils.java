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
package org.apache.asterix.external.util;

import static org.apache.asterix.common.exceptions.ErrorCode.REQUIRED_PARAM_IF_PARAM_IS_PRESENT;
import static org.apache.asterix.external.util.ExternalDataUtils.validateIncludeExclude;
import static org.apache.asterix.om.utils.ProjectionFiltrationTypeUtil.ALL_FIELDS_TYPE;
import static org.apache.asterix.om.utils.ProjectionFiltrationTypeUtil.EMPTY_TYPE;
import static org.apache.hyracks.api.util.ExceptionUtils.getMessageOrToString;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.config.DatasetConfig.ExternalFilePendingOp;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.external.indexing.ExternalFile;
import org.apache.asterix.external.input.record.reader.hdfs.parquet.MapredParquetInputFormat;
import org.apache.asterix.external.input.record.reader.hdfs.parquet.ParquetReadSupport;
import org.apache.asterix.external.input.stream.HDFSInputStream;
import org.apache.asterix.external.util.ExternalDataConstants.ParquetOptions;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.runtime.projection.ExternalDatasetProjectionFiltrationInfo;
import org.apache.asterix.runtime.projection.FunctionCallInformation;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.application.ICCServiceContext;
import org.apache.hyracks.api.context.ICCContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.hyracks.api.network.INetworkSecurityManager;
import org.apache.hyracks.hdfs.scheduler.Scheduler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.hadoop.ParquetInputFormat;

public class HDFSUtils {

    private static final Logger LOGGER = LogManager.getLogger();

    private HDFSUtils() {
    }

    public static Scheduler initializeHDFSScheduler(ICCServiceContext serviceCtx) throws HyracksDataException {
        ICCContext ccContext = serviceCtx.getCCContext();
        INetworkSecurityManager networkSecurityManager = serviceCtx.getControllerService().getNetworkSecurityManager();
        Scheduler scheduler = null;
        try {
            scheduler = new Scheduler(ccContext.getClusterControllerInfo().getClientNetAddress(),
                    ccContext.getClusterControllerInfo().getClientNetPort(),
                    networkSecurityManager.getSocketChannelFactory());
        } catch (HyracksException e) {
            throw new RuntimeDataException(ErrorCode.UTIL_HDFS_UTILS_CANNOT_OBTAIN_HDFS_SCHEDULER);
        }
        return scheduler;
    }

    /**
     * Instead of creating the split using the input format, we do it manually
     * This function returns fileSplits (1 per hdfs file block) irrespective of the number of partitions
     * and the produced splits only cover intersection between current files in hdfs and files stored internally
     * in AsterixDB
     * 1. NoOp means appended file
     * 2. AddOp means new file
     * 3. UpdateOp means the delta of a file
     *
     * @return
     * @throws IOException
     */
    public static InputSplit[] getSplits(JobConf conf, List<ExternalFile> files) throws IOException {
        // Create file system object
        FileSystem fs = FileSystem.get(conf);
        ArrayList<FileSplit> fileSplits = new ArrayList<>();
        ArrayList<ExternalFile> orderedExternalFiles = new ArrayList<>();
        // Create files splits
        for (ExternalFile file : files) {
            Path filePath = new Path(file.getFileName());
            FileStatus fileStatus;
            try {
                fileStatus = fs.getFileStatus(filePath);
            } catch (FileNotFoundException e) {
                // file was deleted at some point, skip to next file
                continue;
            }
            if (file.getPendingOp() == ExternalFilePendingOp.ADD_OP
                    && fileStatus.getModificationTime() == file.getLastModefiedTime().getTime()) {
                // Get its information from HDFS name node
                BlockLocation[] fileBlocks = fs.getFileBlockLocations(fileStatus, 0, file.getSize());
                // Create a split per block
                for (BlockLocation block : fileBlocks) {
                    if (block.getOffset() < file.getSize()) {
                        fileSplits
                                .add(new FileSplit(filePath,
                                        block.getOffset(), (block.getLength() + block.getOffset()) < file.getSize()
                                                ? block.getLength() : (file.getSize() - block.getOffset()),
                                        block.getHosts()));
                        orderedExternalFiles.add(file);
                    }
                }
            } else if (file.getPendingOp() == ExternalFilePendingOp.NO_OP
                    && fileStatus.getModificationTime() == file.getLastModefiedTime().getTime()) {
                long oldSize = 0L;
                long newSize = file.getSize();
                for (int i = 0; i < files.size(); i++) {
                    if (files.get(i).getFileName() == file.getFileName() && files.get(i).getSize() != file.getSize()) {
                        newSize = files.get(i).getSize();
                        oldSize = file.getSize();
                        break;
                    }
                }

                // Get its information from HDFS name node
                BlockLocation[] fileBlocks = fs.getFileBlockLocations(fileStatus, 0, newSize);
                // Create a split per block
                for (BlockLocation block : fileBlocks) {
                    if (block.getOffset() + block.getLength() > oldSize) {
                        if (block.getOffset() < newSize) {
                            // Block interact with delta -> Create a split
                            long startCut = (block.getOffset() > oldSize) ? 0L : oldSize - block.getOffset();
                            long endCut = (block.getOffset() + block.getLength() < newSize) ? 0L
                                    : block.getOffset() + block.getLength() - newSize;
                            long splitLength = block.getLength() - startCut - endCut;
                            fileSplits.add(new FileSplit(filePath, block.getOffset() + startCut, splitLength,
                                    block.getHosts()));
                            orderedExternalFiles.add(file);
                        }
                    }
                }
            }
        }
        fs.close();
        files.clear();
        files.addAll(orderedExternalFiles);
        return fileSplits.toArray(new FileSplit[fileSplits.size()]);
    }

    public static String getInputFormatClassName(Map<String, String> configuration) {
        String inputFormatParameter = configuration.get(ExternalDataConstants.KEY_INPUT_FORMAT).trim();
        switch (inputFormatParameter) {
            case ExternalDataConstants.INPUT_FORMAT_TEXT:
                return ExternalDataConstants.CLASS_NAME_TEXT_INPUT_FORMAT;
            case ExternalDataConstants.INPUT_FORMAT_SEQUENCE:
                return ExternalDataConstants.CLASS_NAME_SEQUENCE_INPUT_FORMAT;
            case ExternalDataConstants.INPUT_FORMAT_PARQUET:
                return ExternalDataConstants.CLASS_NAME_PARQUET_INPUT_FORMAT;
            default:
                return inputFormatParameter;
        }
    }

    public static Class<?> getInputFormatClass(Map<String, String> configuration) throws ClassNotFoundException {
        String inputFormatParameter = configuration.get(ExternalDataConstants.KEY_INPUT_FORMAT).trim();
        switch (inputFormatParameter) {
            case ExternalDataConstants.INPUT_FORMAT_TEXT:
                return TextInputFormat.class;
            case ExternalDataConstants.INPUT_FORMAT_SEQUENCE:
                return SequenceFileInputFormat.class;
            case ExternalDataConstants.INPUT_FORMAT_PARQUET:
                return MapredParquetInputFormat.class;
            default:
                return Class.forName(inputFormatParameter);
        }
    }

    public static JobConf configureHDFSJobConf(Map<String, String> configuration) {
        JobConf conf = new JobConf();
        String localShortCircuitSocketPath = configuration.get(ExternalDataConstants.KEY_LOCAL_SOCKET_PATH);
        String formatClassName = HDFSUtils.getInputFormatClassName(configuration);
        String url = configuration.get(ExternalDataConstants.KEY_HDFS_URL);

        //Allow hdfs adapter to read from local-files. However, this only works in a single-node configuration.
        if (url != null && url.trim().startsWith("hdfs")) {
            conf.set(ExternalDataConstants.KEY_HADOOP_FILESYSTEM_CLASS,
                    ExternalDataConstants.CLASS_NAME_HDFS_FILESYSTEM);
            conf.set(ExternalDataConstants.KEY_HADOOP_FILESYSTEM_URI, url);
        }

        // In case KEY_PATH is not being used, it means DEFINITION_FIELD_NAME is being used,
        // and the property KEY_HADOOP_INPUT_DIR will be set in HDFSDatasourceFactory based on the provided definition.
        String path = configuration.get(ExternalDataConstants.KEY_PATH);
        if (path != null) {
            HDFSUtils.setInputDir(conf, path.trim());
        }
        conf.setClassLoader(HDFSInputStream.class.getClassLoader());
        conf.set(ExternalDataConstants.KEY_HADOOP_INPUT_FORMAT, formatClassName);

        // Enable local short circuit reads if user supplied the parameters
        if (localShortCircuitSocketPath != null) {
            conf.set(ExternalDataConstants.KEY_HADOOP_SHORT_CIRCUIT, "true");
            conf.set(ExternalDataConstants.KEY_HADOOP_SOCKET_PATH, localShortCircuitSocketPath.trim());
        }

        if (ExternalDataConstants.CLASS_NAME_PARQUET_INPUT_FORMAT.equals(formatClassName)) {
            configureParquet(configuration, conf);
        }

        if (configuration.containsKey(ExternalDataConstants.S3A_CHANGE_DETECTION_REQUIRED)) {
            conf.set(ExternalDataConstants.S3A_CHANGE_DETECTION_REQUIRED_CONFIG_KEY,
                    configuration.get(ExternalDataConstants.S3A_CHANGE_DETECTION_REQUIRED));
        }

        String useDatanodeHostname = configuration.get(ExternalDataConstants.HDFS_USE_DATANODE_HOSTNAME);
        if (useDatanodeHostname != null) {
            conf.set(ExternalDataConstants.KEY_HDFS_USE_DATANODE_HOSTNAME, useDatanodeHostname);
        }
        return conf;
    }

    public static Configuration configureHDFSwrite(Map<String, String> configuration) {
        Configuration conf = new Configuration();
        String url = configuration.get(ExternalDataConstants.KEY_HDFS_URL);
        String blocksize = configuration.get(ExternalDataConstants.HDFS_BLOCKSIZE);
        String replication = configuration.get(ExternalDataConstants.HDFS_REPLICATION);
        String useDatanodeHostname = configuration.get(ExternalDataConstants.HDFS_USE_DATANODE_HOSTNAME);
        if (url != null) {
            conf.set(ExternalDataConstants.KEY_HADOOP_FILESYSTEM_URI, url);
        }
        if (blocksize != null) {
            conf.set(ExternalDataConstants.KEY_HDFS_BLOCKSIZE, blocksize);
        }
        if (replication != null) {
            conf.set(ExternalDataConstants.KEY_HDFS_REPLICATION, replication);
        }
        if (useDatanodeHostname != null) {
            conf.set(ExternalDataConstants.KEY_HDFS_USE_DATANODE_HOSTNAME, useDatanodeHostname);
        }
        return conf;
    }

    public synchronized static Credentials configureHadoopAuthentication(Map<String, String> configuration,
            Configuration conf) throws AlgebricksException {
        UserGroupInformation.reset();
        if (Objects.equals(configuration.get(ExternalDataConstants.HADOOP_AUTHENTICATION),
                ExternalDataConstants.KERBEROS_PROTOCOL)) {
            conf.set(ExternalDataConstants.KEY_HADOOP_AUTHENTICATION, ExternalDataConstants.KERBEROS_PROTOCOL);
            conf.set(ExternalDataConstants.KEY_NAMENODE_PRINCIPAL_PATTERN, "*");

            String kerberosRealm = configuration.get(ExternalDataConstants.KERBEROS_REALM);
            String kerberosKdc = configuration.get(ExternalDataConstants.KERBEROS_KDC);
            String kerberosPrincipal = configuration.get(ExternalDataConstants.KERBEROS_PRINCIPAL);
            String kerberosPassword = configuration.get(ExternalDataConstants.KERBEROS_PASSWORD);

            javax.security.auth.login.Configuration config = new JaasConfiguration();
            java.nio.file.Path krb5conf = null;
            try {
                krb5conf = createKerberosConf(kerberosRealm, kerberosKdc);
                System.setProperty(ExternalDataConstants.KEY_KERBEROS_CONF, krb5conf.toString());

                Subject subject = new Subject();
                LoginContext loginContext =
                        new LoginContext("", subject, new callbackHandler(kerberosPrincipal, kerberosPassword), config);
                loginContext.login();

                // Hadoop libraries use rules to ensure that a principal is associated with a realm.
                // The default realm is static and needs to be modified in order to work with a different realm.
                KerberosName.resetDefaultRealm();
                UserGroupInformation.setConfiguration(conf);
                UserGroupInformation ugi = UserGroupInformation.getUGIFromSubject(subject);
                Credentials credentials = new Credentials();
                ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
                    try (FileSystem fs = FileSystem.get(conf)) {
                        fs.addDelegationTokens(ugi.getUserName(), credentials);
                    }
                    return null;
                });
                loginContext.logout();
                return credentials;
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw CompilationException.create(ErrorCode.COULD_NOT_CREATE_TOKENS);
            } catch (LoginException | IOException ex) {
                throw new CompilationException(ErrorCode.EXTERNAL_SOURCE_ERROR, ex, getMessageOrToString(ex));
            } finally {
                System.clearProperty(ExternalDataConstants.KEY_KERBEROS_CONF);
                UserGroupInformation.reset();
                if (krb5conf != null) {
                    FileUtils.deleteQuietly(krb5conf.toFile());
                }
            }
        }
        return null;
    }

    private static java.nio.file.Path createKerberosConf(String kerberosRealm, String kerberosKdc)
            throws AlgebricksException {
        java.nio.file.Path krb5conf;
        try {
            krb5conf = Files.createTempFile(ExternalDataConstants.KERBEROS_CONFIG_FILE_PATTERN[0],
                    ExternalDataConstants.KERBEROS_CONFIG_FILE_PATTERN[1]);
            Files.writeString(krb5conf,
                    String.format(ExternalDataConstants.KERBEROS_CONFIG_FILE_CONTENT, kerberosRealm, kerberosKdc));
        } catch (IOException ex) {
            LOGGER.error("Failed to create kerberos config file", ex);
            throw AsterixException.create(ErrorCode.COULD_NOT_CREATE_TOKENS);
        }
        return krb5conf;
    }

    private static class JaasConfiguration extends javax.security.auth.login.Configuration {
        @Override
        public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
            Map<String, Object> options =
                    Map.of(ExternalDataConstants.KERBEROS_CONFIG_REFRESH, ExternalDataConstants.TRUE);

            return new AppConfigurationEntry[] { new AppConfigurationEntry(ExternalDataConstants.KERBEROS_LOGIN_MODULE,
                    AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options) };
        }
    }

    public static byte[] serialize(Writable writable) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                DataOutputStream oos = new DataOutputStream(bos)) {
            writable.write(oos);
            return bos.toByteArray();
        }
    }

    public static void deserialize(byte[] data, Writable writable) throws IOException {
        try (DataInputStream din = new DataInputStream(new ByteArrayInputStream(data))) {
            writable.readFields(din);
        }
    }

    private static class callbackHandler implements CallbackHandler {
        private final String principal;
        private final String password;

        public callbackHandler(String principal, String password) {
            this.principal = principal;
            this.password = password;
        }

        @Override
        public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
            for (Callback callback : callbacks) {
                if (callback instanceof NameCallback) {
                    ((NameCallback) callback).setName(principal);
                } else if (callback instanceof PasswordCallback) {
                    ((PasswordCallback) callback).setPassword(password.toCharArray());
                } else {
                    throw new UnsupportedCallbackException(callback);
                }
            }
        }
    }

    public static String updateRootPath(String path, boolean updateNullPath) {
        if (path != null && path.isEmpty()) {
            return "/";
        }
        return path != null ? path : (updateNullPath ? "/" : null);
    }

    private static void configureParquet(Map<String, String> configuration, JobConf conf) {
        //Parquet configurations
        conf.set(ParquetInputFormat.READ_SUPPORT_CLASS, ParquetReadSupport.class.getName());

        //Get requested values
        String requestedValues = configuration.get(ExternalDataConstants.KEY_REQUESTED_FIELDS);
        if (requestedValues == null) {
            //No value is requested, return the entire record
            requestedValues = ALL_FIELDS_TYPE.getTypeName();
        } else {
            //Subset of the values were requested, set the functionCallInformation
            conf.set(ExternalDataConstants.KEY_HADOOP_ASTERIX_FUNCTION_CALL_INFORMATION,
                    configuration.get(ExternalDataConstants.KEY_HADOOP_ASTERIX_FUNCTION_CALL_INFORMATION));
        }
        conf.set(ExternalDataConstants.KEY_REQUESTED_FIELDS, requestedValues);

        //Parse JSON string as ADM?
        conf.set(ParquetOptions.HADOOP_PARSE_JSON_STRING,
                configuration.getOrDefault(ParquetOptions.PARSE_JSON_STRING, ExternalDataConstants.TRUE));

        //Rebase and parse decimal as double?
        conf.set(ParquetOptions.HADOOP_DECIMAL_TO_DOUBLE,
                configuration.getOrDefault(ParquetOptions.DECIMAL_TO_DOUBLE, ExternalDataConstants.FALSE));
        //Re-adjust the time zone for UTC-adjusted values
        conf.set(ParquetOptions.HADOOP_TIMEZONE, configuration.getOrDefault(ParquetOptions.TIMEZONE, ""));

    }

    public static AlgebricksAbsolutePartitionConstraint getPartitionConstraints(IApplicationContext appCtx,
            AlgebricksAbsolutePartitionConstraint clusterLocations) {
        if (clusterLocations == null) {
            return ((ICcApplicationContext) appCtx).getDataPartitioningProvider().getClusterLocations();
        }
        return clusterLocations;

    }

    public static ARecordType getExpectedType(Configuration configuration) throws IOException {
        String encoded = configuration.get(ExternalDataConstants.KEY_REQUESTED_FIELDS, "");
        if (ALL_FIELDS_TYPE.getTypeName().equals(encoded)) {
            //By default, return the entire records
            return ALL_FIELDS_TYPE;
        } else if (EMPTY_TYPE.getTypeName().equals(encoded)) {
            //No fields were requested
            return EMPTY_TYPE;
        }
        //A subset of the fields was requested
        Base64.Decoder decoder = Base64.getDecoder();
        byte[] typeBytes = decoder.decode(encoded);
        DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(typeBytes));
        return ExternalDatasetProjectionFiltrationInfo.createTypeField(dataInputStream);
    }

    public static void setFunctionCallInformationMap(Map<String, FunctionCallInformation> funcCallInfoMap,
            Configuration conf) throws IOException {
        String stringFunctionCallInfoMap = ExternalDataUtils.serializeFunctionCallInfoToString(funcCallInfoMap);
        conf.set(ExternalDataConstants.KEY_HADOOP_ASTERIX_FUNCTION_CALL_INFORMATION, stringFunctionCallInfoMap);
    }

    public static Map<String, FunctionCallInformation> getFunctionCallInformationMap(Configuration conf)
            throws IOException {
        String encoded = conf.get(ExternalDataConstants.KEY_HADOOP_ASTERIX_FUNCTION_CALL_INFORMATION, "");
        if (!encoded.isEmpty()) {
            Base64.Decoder decoder = Base64.getDecoder();
            byte[] functionCallInfoMapBytes = decoder.decode(encoded);
            DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(functionCallInfoMapBytes));
            return ExternalDatasetProjectionFiltrationInfo.createFunctionCallInformationMap(dataInputStream);
        }
        return null;
    }

    public static void setWarnings(List<Warning> warnings, Configuration conf) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);

        StringBuilder stringBuilder = new StringBuilder();
        Base64.Encoder encoder = Base64.getEncoder();
        for (int i = 0; i < warnings.size(); i++) {
            Warning warning = warnings.get(i);
            warning.writeFields(dataOutputStream);
            stringBuilder.append(encoder.encodeToString(byteArrayOutputStream.toByteArray()));
            //Warnings are separated by ','
            stringBuilder.append(',');
            byteArrayOutputStream.reset();
        }
        conf.set(ExternalDataConstants.KEY_HADOOP_ASTERIX_WARNINGS_LIST, stringBuilder.toString());
    }

    public static void issueWarnings(IWarningCollector warningCollector, Configuration conf) throws IOException {
        String warnings = conf.get(ExternalDataConstants.KEY_HADOOP_ASTERIX_WARNINGS_LIST, "");
        if (!warnings.isEmpty()) {
            String[] encodedWarnings = warnings.split(",");
            Base64.Decoder decoder = Base64.getDecoder();
            for (int i = 0; i < encodedWarnings.length; i++) {
                /*
                 * This should create a small number of objects as warnings are reported only once by AsterixDB's
                 * hadoop readers
                 */
                byte[] warningBytes = decoder.decode(encodedWarnings[i]);
                DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(warningBytes));
                if (warningCollector.shouldWarn()) {
                    warningCollector.warn(Warning.create(dataInputStream));
                }
            }
            //Remove reported warnings
            conf.unset(ExternalDataConstants.KEY_HADOOP_ASTERIX_WARNINGS_LIST);
        }
    }

    /**
     * Hadoop can cache FileSystem instance if reading the same file. This method allows for disabling the cache
     *
     * @param conf     Hadoop configuration
     * @param protocol fs scheme (or protocol). e.g., s3a
     */
    public static void disableHadoopFileSystemCache(Configuration conf, String protocol) {
        //Disable fs cache
        conf.set(String.format(ExternalDataConstants.KEY_HADOOP_DISABLE_FS_CACHE_TEMPLATE, protocol),
                ExternalDataConstants.TRUE);
    }

    /**
     * Check whether the provided path is empty
     *
     * @param job Hadoop Configuration
     * @return <code>true</code> if the path is empty, <code>false</code> otherwise
     */
    public static boolean isEmpty(JobConf job) {
        return job.get(ExternalDataConstants.KEY_HADOOP_INPUT_DIR, "").isEmpty();
    }

    public static void setInputDir(JobConf conf, String path) {
        conf.set(ExternalDataConstants.KEY_HADOOP_INPUT_DIR, path);
    }

    public static void validateProperties(Map<String, String> configuration, SourceLocation srcLoc,
            IWarningCollector collector) throws CompilationException {
        if (configuration.get(ExternalDataConstants.KEY_INPUT_FORMAT) == null) {
            throw new CompilationException(ErrorCode.PARAMETERS_REQUIRED, srcLoc,
                    ExternalDataConstants.KEY_INPUT_FORMAT);
        }

        if (Objects.equals(configuration.get(ExternalDataConstants.HADOOP_AUTHENTICATION),
                ExternalDataConstants.KERBEROS_PROTOCOL)) {
            String kerberosRealm = configuration.get(ExternalDataConstants.KERBEROS_REALM);
            String kerberosKdc = configuration.get(ExternalDataConstants.KERBEROS_KDC);
            String kerberosPrincipal = configuration.get(ExternalDataConstants.KERBEROS_PRINCIPAL);
            String kerberosPassword = configuration.get(ExternalDataConstants.KERBEROS_PASSWORD);

            if (kerberosRealm == null) {
                throw CompilationException.create(REQUIRED_PARAM_IF_PARAM_IS_PRESENT,
                        ExternalDataConstants.KERBEROS_REALM, ExternalDataConstants.HADOOP_AUTHENTICATION);
            }
            if (kerberosKdc == null) {
                throw CompilationException.create(REQUIRED_PARAM_IF_PARAM_IS_PRESENT,
                        ExternalDataConstants.KERBEROS_KDC, ExternalDataConstants.HADOOP_AUTHENTICATION);
            }
            if (kerberosPrincipal == null) {
                throw CompilationException.create(REQUIRED_PARAM_IF_PARAM_IS_PRESENT,
                        ExternalDataConstants.KERBEROS_PRINCIPAL, ExternalDataConstants.HADOOP_AUTHENTICATION);
            }
            if (kerberosPassword == null) {
                throw CompilationException.create(REQUIRED_PARAM_IF_PARAM_IS_PRESENT,
                        ExternalDataConstants.KERBEROS_PASSWORD, ExternalDataConstants.HADOOP_AUTHENTICATION);
            }
        }

        // For validation purposes for external data prefixes
        validateIncludeExclude(configuration);
        try {
            new ExternalDataPrefix(configuration);
        } catch (AlgebricksException ex) {
            throw new CompilationException(ErrorCode.FAILED_TO_CALCULATE_COMPUTED_FIELDS, ex);
        }
    }

    public static boolean isSourceTypeHdfs(Map<String, String> configuration) {
        return ExternalDataConstants.KEY_ADAPTER_NAME_HDFS
                .equalsIgnoreCase(configuration.get(ExternalDataConstants.KEY_EXTERNAL_SOURCE_TYPE));
    }
}
