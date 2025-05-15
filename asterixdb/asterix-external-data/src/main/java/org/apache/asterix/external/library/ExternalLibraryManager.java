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
package org.apache.asterix.external.library;

import static com.fasterxml.jackson.databind.MapperFeature.SORT_PROPERTIES_ALPHABETICALLY;
import static com.fasterxml.jackson.databind.SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.apache.asterix.external.library.PythonLibraryTCPSocketEvaluator.ENTRYPOINT;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.DigestOutputStream;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import javax.net.ssl.SSLContext;

import org.apache.asterix.common.api.INamespacePathResolver;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.functions.ExternalFunctionLanguage;
import org.apache.asterix.common.library.ILibrary;
import org.apache.asterix.common.library.ILibraryManager;
import org.apache.asterix.common.library.LibraryDescriptor;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.MetadataConstants;
import org.apache.asterix.common.metadata.Namespace;
import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.asterix.external.ipc.ExternalFunctionResultRouter;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IFileHandle;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;
import org.apache.hyracks.api.lifecycle.ILifeCycleComponent;
import org.apache.hyracks.api.network.INetworkSecurityConfig;
import org.apache.hyracks.api.network.INetworkSecurityManager;
import org.apache.hyracks.api.util.IoUtil;
import org.apache.hyracks.cloud.io.ICloudIOManager;
import org.apache.hyracks.control.common.work.AbstractWork;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.ipc.impl.IPCSystem;
import org.apache.hyracks.ipc.security.NetworkSecurityManager;
import org.apache.hyracks.ipc.sockets.PlainSocketChannelFactory;
import org.apache.hyracks.util.file.FileUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.io.ByteStreams;

public class ExternalLibraryManager implements ILibraryManager, ILifeCycleComponent {

    public static final String LIBRARY_MANAGER_BASE_DIR_NAME = "library";

    private static final String STORAGE_DIR_NAME = "storage";

    private static final String TRASH_DIR_NAME = "trash";

    public static final String REV_0_DIR_NAME = "rev_0";

    public static final String REV_1_DIR_NAME = "rev_1";

    public static final String STAGE_DIR_NAME = "stage";

    public static final String CONTENTS_DIR_NAME = "contents";

    public static final String DESCRIPTOR_FILE_NAME = "desc.json";

    public static final String DISTRIBUTION_DIR = "dist";

    private static final int DOWNLOAD_RETRY_COUNT = 10;

    private static final Logger LOGGER = LogManager.getLogger(ExternalLibraryManager.class);

    private final NodeControllerService ncs;
    private final IPersistedResourceRegistry reg;
    private final ObjectMapper objectMapper;
    private final FileReference baseDir;
    private final FileReference storageDir;
    private final Path storageDirPath;
    private final FileReference trashDir;
    private final FileReference distDir;
    private final Path trashDirPath;
    private final Map<Pair<Namespace, String>, ILibrary> libraries = new HashMap<>();
    private IPCSystem pythonIPC;
    private final ExternalFunctionResultRouter router;
    private final IIOManager ioManager;
    private final INamespacePathResolver namespacePathResolver;
    private final boolean sslEnabled;
    private final boolean cloudMode;
    private final long maxFileSize;
    private final long maxTotalSize;
    private final int maxEntries;
    private Function<ILibraryManager, CloseableHttpClient> uploadClientSupp;

    public ExternalLibraryManager(NodeControllerService ncs, IPersistedResourceRegistry reg, FileReference appDir,
            IIOManager ioManager) {
        this.ncs = ncs;
        this.reg = reg;
        namespacePathResolver = ((INcApplicationContext) ncs.getApplicationContext()).getNamespacePathResolver();
        baseDir = appDir.getChild(LIBRARY_MANAGER_BASE_DIR_NAME);
        storageDir = baseDir.getChild(STORAGE_DIR_NAME);
        storageDirPath = storageDir.getFile().toPath();
        trashDir = baseDir.getChild(TRASH_DIR_NAME);
        distDir = baseDir.getChild(DISTRIBUTION_DIR);
        trashDirPath = trashDir.getFile().toPath().normalize();
        objectMapper = createObjectMapper();
        router = new ExternalFunctionResultRouter();
        this.sslEnabled = ncs.getConfiguration().isSslEnabled();
        this.ioManager = ioManager;
        uploadClientSupp = ExternalLibraryManager::defaultHttpClient;
        cloudMode = ncs.getConfiguration().isCloudDeployment();
        maxFileSize = ncs.getConfiguration().getLibraryMaxFileSize();
        maxTotalSize = ncs.getConfiguration().getLibraryMaxExtractedSize();
        maxEntries = ncs.getConfiguration().getLibraryMaxArchiveEntries();
    }

    public void initialize(boolean resetStorageData) throws HyracksDataException {
        try {
            pythonIPC = new IPCSystem(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0),
                    PlainSocketChannelFactory.INSTANCE, router, new ExternalFunctionResultRouter.NoOpNoSerJustDe());
            pythonIPC.start();
            Path baseDirPath = baseDir.getFile().toPath();
            if (Files.isDirectory(baseDirPath)) {
                if (resetStorageData) {
                    FileUtils.cleanDirectory(baseDir.getFile());
                    Files.createDirectory(storageDirPath);
                    Files.createDirectory(trashDirPath);
                    IoUtil.flushDirectory(baseDirPath);
                } else {
                    boolean createdDirs = false;
                    if (!Files.isDirectory(storageDirPath)) {
                        Files.deleteIfExists(storageDirPath);
                        Files.createDirectory(storageDirPath);
                        createdDirs = true;
                    }
                    if (Files.isDirectory(trashDirPath)) {
                        FileUtils.cleanDirectory(trashDir.getFile());
                    } else {
                        Files.deleteIfExists(trashDirPath);
                        Files.createDirectory(trashDirPath);
                        createdDirs = true;
                    }
                    //TODO:clean all rev_0 if their rev_1 exist
                    if (createdDirs) {
                        IoUtil.flushDirectory(baseDirPath);
                    }
                }
            } else {
                FileUtil.forceMkdirs(baseDir.getFile());
                Files.createDirectory(storageDirPath);
                Files.createDirectory(trashDirPath);
                // flush app dir's parent because we might've created app dir there
                IoUtil.flushDirectory(baseDirPath.getParent().getParent());
                // flush app dir (base dir's parent) because we might've created base dir there
                IoUtil.flushDirectory(baseDirPath.getParent());
                // flush base dir because we created storage/trash dirs there
                IoUtil.flushDirectory(baseDirPath);
            }
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public void start() {
        if (cloudMode) {
            try {
                unzipAllLibs(baseDir);
            } catch (IOException e) {
                LOGGER.error("Failed to unzip all libraries", e);
            }
        }
    }

    @Override
    public void stop(boolean dumpState, OutputStream ouputStream) {
        synchronized (this) {
            for (Map.Entry<Pair<Namespace, String>, ILibrary> p : libraries.entrySet()) {
                ILibrary library = p.getValue();
                try {
                    library.close();
                } catch (HyracksDataException e) {
                    LOGGER.warn("Error closing library " + p.getKey().first + "." + p.getKey().second, e);
                }
            }
        }
    }

    @Override
    public FileReference getStorageDir() {
        return storageDir;
    }

    private FileReference getDataverseDir(Namespace namespace) throws HyracksDataException {
        return getChildFileRef(storageDir, namespacePathResolver.resolve(namespace));
    }

    @Override
    public FileReference getLibraryDir(Namespace namespace, String libraryName) throws HyracksDataException {
        FileReference dataverseDir = getDataverseDir(namespace);
        return getChildFileRef(dataverseDir, libraryName);
    }

    @Override
    public FileReference getDistributionDir() {
        return distDir;
    }

    @Override
    public List<Pair<Namespace, String>> getLibraryListing() throws IOException {
        List<Pair<Namespace, String>> libs = new ArrayList<>();
        Files.walkFileTree(storageDirPath, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path currPath, BasicFileAttributes attrs) {
                //never want to see any files
                return FileVisitResult.TERMINATE;
            }

            @Override
            public FileVisitResult preVisitDirectory(Path currPath, BasicFileAttributes attrs)
                    throws HyracksDataException {
                if (currPath.equals(storageDirPath) || currPath.getParent().equals(storageDirPath)) {
                    return FileVisitResult.CONTINUE;
                }
                if (currPath.getFileName().toString().codePointAt(0) == StoragePathUtil.DATAVERSE_CONTINUATION_MARKER) {
                    return FileVisitResult.CONTINUE;
                }
                final String candidateDvAndLib = storageDirPath.toAbsolutePath().normalize()
                        .relativize(currPath.toAbsolutePath().normalize()).toString();
                List<String> dvParts = new ArrayList<>();
                final String[] tokens = StringUtils.split(candidateDvAndLib, File.separatorChar);
                if (tokens == null || tokens.length < 2) {
                    //? shouldn't happen
                    return FileVisitResult.TERMINATE;
                }
                try {
                    String candidateDb = MetadataConstants.DEFAULT_DATABASE;
                    if (namespacePathResolver.usingDatabase()) {
                        if (tokens.length < 3) {
                            return FileVisitResult.TERMINATE;
                        }
                        libs.add(new Pair<>(new Namespace(candidateDb, DataverseName.create(List.of(tokens[1]))),
                                tokens[3]));
                        return FileVisitResult.SKIP_SUBTREE;

                    }
                    //add first part, then look for multiparts
                    dvParts.add(tokens[0]);
                    int currToken = 1;
                    for (; currToken < tokens.length && tokens[currToken]
                            .codePointAt(0) == StoragePathUtil.DATAVERSE_CONTINUATION_MARKER; currToken++) {
                        dvParts.add(tokens[currToken].substring(1));
                    }
                    //we should only arrive at foo/^bar/^baz/.../^bat/lib
                    //anything else is fishy or empty
                    if (currToken != tokens.length - 1) {
                        return FileVisitResult.SKIP_SUBTREE;
                    }
                    String candidateLib = tokens[currToken];
                    DataverseName candidateDv;
                    candidateDv = DataverseName.create(dvParts);
                    Namespace candidateNs = new Namespace(candidateDb, candidateDv);
                    FileReference candidateLibPath = findLibraryRevDir(candidateNs, candidateLib);
                    if (candidateLibPath != null) {
                        libs.add(new Pair<>(candidateNs, candidateLib));
                    }
                } catch (AsterixException e) {
                    // shouldn't happen
                    throw HyracksDataException.create(e);
                }
                return FileVisitResult.SKIP_SUBTREE;
            }
        });
        return libs;
    }

    @Override
    public String getLibraryHash(Namespace namespace, String libraryName) throws IOException {
        FileReference revDir = findLibraryRevDir(namespace, libraryName);
        if (revDir == null) {
            throw HyracksDataException
                    .create(AsterixException.create(ErrorCode.EXTERNAL_UDF_EXCEPTION, "Library does not exist"));
        }
        LibraryDescriptor desc = getLibraryDescriptor(revDir);
        return desc.getHash();
    }

    @Override
    public ILibrary getLibrary(Namespace namespace, String libraryName) throws HyracksDataException {
        Pair<Namespace, String> key = getKey(namespace, libraryName);
        synchronized (this) {
            ILibrary library = libraries.get(key);
            if (library == null) {
                library = loadLibrary(namespace, libraryName);
                libraries.put(key, library);
            }
            return library;
        }
    }

    private ILibrary loadLibrary(Namespace namespace, String libraryName) throws HyracksDataException {
        FileReference libRevDir = findLibraryRevDir(namespace, libraryName);
        if (libRevDir == null) {
            throw new HyracksDataException("Cannot find library: " + namespace + '.' + libraryName);
        }
        FileReference libContentsDir = libRevDir.getChild(CONTENTS_DIR_NAME);
        if (!libContentsDir.getFile().isDirectory()) {
            throw new HyracksDataException("Cannot find library: " + namespace + '.' + libraryName);
        }
        try {
            ExternalFunctionLanguage libLang = getLibraryDescriptor(libRevDir).getLanguage();
            switch (libLang) {
                case JAVA:
                    return new JavaLibrary(libContentsDir.getFile());
                case PYTHON:
                    return new PythonLibrary(libContentsDir.getFile());
                default:
                    throw new HyracksDataException("Invalid language: " + libraryName);
            }
        } catch (IOException e) {
            LOGGER.error("Failed to initialize library " + namespace + '.' + libraryName, e);
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public byte[] serializeLibraryDescriptor(LibraryDescriptor libraryDescriptor) throws HyracksDataException {
        try {
            return objectMapper.writeValueAsBytes(libraryDescriptor.toJson(reg));
        } catch (JsonProcessingException e) {
            throw HyracksDataException.create(e);
        }
    }

    private LibraryDescriptor deserializeLibraryDescriptor(byte[] data) throws IOException {
        JsonNode jsonNode = objectMapper.readValue(data, JsonNode.class);
        return (LibraryDescriptor) reg.deserialize(jsonNode);
    }

    private LibraryDescriptor getLibraryDescriptor(FileReference revDir) throws IOException {
        FileReference descFile = revDir.getChild(DESCRIPTOR_FILE_NAME);
        byte[] descData = Files.readAllBytes(descFile.getFile().toPath());
        return deserializeLibraryDescriptor(descData);

    }

    private FileReference findLibraryRevDir(Namespace namespace, String libraryName) throws HyracksDataException {
        FileReference libraryBaseDir = getLibraryDir(namespace, libraryName);
        if (!libraryBaseDir.getFile().isDirectory()) {
            return null;
        }
        FileReference libDirRev1 = libraryBaseDir.getChild(REV_1_DIR_NAME);
        if (libDirRev1.getFile().isDirectory()) {
            return libDirRev1;
        }
        FileReference libDirRev0 = libraryBaseDir.getChild(REV_0_DIR_NAME);
        if (libDirRev0.getFile().isDirectory()) {
            return libDirRev0;
        }
        return null;
    }

    @Override
    public void closeLibrary(Namespace namespace, String libraryName) throws HyracksDataException {
        Pair<Namespace, String> key = getKey(namespace, libraryName);
        ILibrary library;
        synchronized (this) {
            library = libraries.remove(key);
        }
        if (library != null) {
            library.close();
        }
    }

    @Override
    public void dumpState(OutputStream os) {
    }

    private static Pair<Namespace, String> getKey(Namespace namespace, String libraryName) {
        return new Pair<>(namespace, libraryName);
    }

    @Override
    public String getNsOrDv(Namespace ns) {
        if (namespacePathResolver.usingDatabase()) {
            return ns.toString();
        }
        return ns.getDataverseName().toString();
    }

    public Path zipAllLibs() throws IOException {
        byte[] copyBuf = new byte[4096];
        Path outDir = Paths.get(baseDir.getAbsolutePath(), DISTRIBUTION_DIR);
        FileUtil.forceMkdirs(outDir.toFile());
        Path outZip = Files.createTempFile(outDir, "all_", ".zip");
        try (FileOutputStream out = new FileOutputStream(outZip.toFile());
                ZipArchiveOutputStream zipOut = new ZipArchiveOutputStream(out)) {
            Files.walkFileTree(storageDirPath, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path currPath, BasicFileAttributes attrs) throws IOException {
                    ZipArchiveEntry e =
                            new ZipArchiveEntry(currPath.toFile(), storageDirPath.relativize(currPath).toString());
                    zipOut.putArchiveEntry(e);
                    try (FileInputStream fileRead = new FileInputStream(currPath.toFile())) {
                        IOUtils.copyLarge(fileRead, zipOut, copyBuf);
                        zipOut.closeArchiveEntry();
                    }
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult preVisitDirectory(Path currPath, BasicFileAttributes attrs) throws IOException {
                    if (currPath.equals(storageDirPath)) {
                        return FileVisitResult.CONTINUE;
                    }
                    ZipArchiveEntry e =
                            new ZipArchiveEntry(currPath.toFile(), storageDirPath.relativize(currPath).toString());
                    zipOut.putArchiveEntry(e);
                    return FileVisitResult.CONTINUE;
                }
            });
            zipOut.finish();
        }
        return outZip;
    }

    private void unzipAllLibs(FileReference libDir) throws IOException {
        byte[] copyBuf = new byte[4096];
        Files.walkFileTree(libDir.getFile().toPath(), new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path currPath, BasicFileAttributes attrs) throws IOException {
                if (currPath.getFileName().toString().equals(LIBRARY_ARCHIVE_NAME)) {
                    FileReference lib = ioManager.resolveAbsolutePath(currPath.toString());
                    FileReference content = lib.getParent().getChild(REV_1_DIR_NAME).getChild(CONTENTS_DIR_NAME);
                    if (!content.getFile().exists()) {
                        FileUtils.forceMkdir(content.getFile());
                    }
                    unzip(lib, content);
                    writeShim(content.getChild(ENTRYPOINT), copyBuf);
                } else if (currPath.getFileName().toString().equals(DESCRIPTOR_FILE_NAME)) {
                    Path revDir = currPath.resolveSibling(REV_1_DIR_NAME);
                    if (!revDir.toFile().exists()) {
                        FileUtils.forceMkdir(revDir.toFile());
                    }
                    Files.copy(currPath, currPath.resolveSibling(REV_1_DIR_NAME).resolve(DESCRIPTOR_FILE_NAME),
                            REPLACE_EXISTING);
                }
                return FileVisitResult.CONTINUE;
            }
        });
    }

    @Override
    public void dropLibraryPath(FileReference fileRef) throws HyracksDataException {
        try {
            Path path = fileRef.getFile().toPath();
            if (ncs.getConfiguration().isCloudDeployment()) {
                ioManager.delete(fileRef.getChild(LIBRARY_ARCHIVE_NAME));
                ioManager.delete(fileRef.getChild(DESCRIPTOR_FILE_NAME));
            }
            Path trashPath = Files.createTempDirectory(trashDirPath, null);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Drop (move) {} into {}", path, trashPath);
            }
            Files.move(path, trashPath, StandardCopyOption.ATOMIC_MOVE);
            ncs.getWorkQueue().schedule(new DeleteDirectoryWork(trashPath));
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    private FileReference getChildFileRef(FileReference dir, String fileName) throws HyracksDataException {
        Path dirPath = dir.getFile().toPath().toAbsolutePath().normalize();
        FileReference fileRef = dir.getChild(fileName);
        Path filePath = fileRef.getFile().toPath().toAbsolutePath().normalize();
        if (!filePath.startsWith(dirPath)) {
            throw new HyracksDataException("Invalid file name: " + fileName);
        }
        return fileRef;
    }

    private static ObjectMapper createObjectMapper() {
        ObjectMapper om = new ObjectMapper();
        om.enable(SerializationFeature.INDENT_OUTPUT);
        om.configure(SORT_PROPERTIES_ALPHABETICALLY, true);
        om.configure(ORDER_MAP_ENTRIES_BY_KEYS, true);
        return om;
    }

    @Override
    public ExternalFunctionResultRouter getRouter() {
        return router;
    }

    @Override
    public IPCSystem getIPCI() {
        return pythonIPC;
    }

    @Override
    public NodeControllerService getNcs() {
        return ncs;
    }

    private static final class DeleteDirectoryWork extends AbstractWork {

        private final Path path;

        private DeleteDirectoryWork(Path path) {
            this.path = path;
        }

        @Override
        public void run() {
            try {
                IoUtil.delete(path);
            } catch (HyracksDataException e) {
                LOGGER.warn("Error deleting " + path);
            }
        }
    }

    @Override
    public MessageDigest download(FileReference targetFile, String authToken, URI libLocation) throws HyracksException {
        try {
            targetFile.getFile().createNewFile();
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
        IFileHandle fHandle = ioManager.open(targetFile, IIOManager.FileReadWriteMode.READ_WRITE,
                IIOManager.FileSyncMode.METADATA_ASYNC_DATA_ASYNC);

        MessageDigest digest = DigestUtils.getDigest("MD5");
        try {
            CloseableHttpClient httpClient = newClient();
            try {
                // retry 10 times at maximum for downloading binaries
                HttpGet request = new HttpGet(libLocation);
                request.setHeader(HttpHeaders.AUTHORIZATION, authToken);
                int tried = 0;
                Exception trace = null;
                while (tried < DOWNLOAD_RETRY_COUNT) {
                    tried++;
                    CloseableHttpResponse response = null;
                    try {
                        response = httpClient.execute(request);
                        if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                            throw new IOException("Http Error: " + response.getStatusLine().getStatusCode());
                        }
                        HttpEntity e = response.getEntity();
                        if (e == null) {
                            throw new IOException("No response");
                        }
                        WritableByteChannel outChannel = ioManager.newWritableChannel(fHandle);
                        OutputStream outStream = new DigestOutputStream(Channels.newOutputStream(outChannel), digest);
                        e.writeTo(outStream);
                        outStream.flush();
                        ioManager.sync(fHandle, true);
                        return digest;
                    } catch (IOException e) {
                        LOGGER.error("Unable to download library", e);
                        trace = e;
                        try {
                            ioManager.truncate(fHandle, 0);
                            digest.reset();
                        } catch (IOException e2) {
                            throw HyracksDataException.create(e2);
                        }
                    } finally {
                        if (response != null) {
                            try {
                                response.close();
                            } catch (IOException e) {
                                LOGGER.warn("Failed to close", e);
                            }
                        }
                    }
                }

                throw HyracksDataException.create(trace);
            } finally {
                try {
                    httpClient.close();
                } catch (IOException e) {
                    LOGGER.warn("Failed to close", e);
                }
            }
        } finally {
            try {
                ioManager.close(fHandle);
            } catch (HyracksDataException e) {
                LOGGER.warn("Failed to close", e);
            }
        }
    }

    @Override
    public void unzip(FileReference sourceFile, FileReference outputDir) throws IOException {
        unzip(sourceFile, outputDir, true);
    }

    @Override
    public void unzip(FileReference sourceFile, FileReference outputDir, boolean limited) throws IOException {
        boolean logTraceEnabled = LOGGER.isTraceEnabled();
        IIOManager localIoManager = ioManager;
        if (ncs.getConfiguration().isCloudDeployment()) {
            localIoManager = ((ICloudIOManager) ioManager).getLocalIOManager();
        }
        Set<Path> newDirs = new HashSet<>();
        Path outputDirPath = outputDir.getFile().toPath().toAbsolutePath().normalize();
        try (ZipFile zipFile = new ZipFile(sourceFile.getFile())) {
            Enumeration<? extends ZipEntry> entries = zipFile.entries();
            byte[] writeBuf = new byte[4096];
            int numEntries = 0;
            long totalSize = 0;
            while (entries.hasMoreElements()) {
                if (limited && numEntries >= maxEntries) {
                    throw new IOException(
                            "Library archive contains more files and directories than configuration permits");
                }
                //may exceed the total allowable size by the maximum size of one file, because we can't know how
                //big the file is until we actually attempt to write it.
                if (limited && totalSize > maxTotalSize) {
                    throw new IOException("Library archive extracted size exceeds maximum configured allowable size");
                }
                ZipEntry entry = entries.nextElement();
                if (entry.isDirectory()) {
                    continue;
                }
                Path entryOutputPath = outputDirPath.resolve(entry.getName()).toAbsolutePath().normalize();
                if (!entryOutputPath.startsWith(outputDirPath)) {
                    throw new IOException("Malformed ZIP archive: " + entry.getName());
                }
                Path entryOutputDir = entryOutputPath.getParent();
                Files.createDirectories(entryOutputDir);
                // remember new directories so we can flush them later
                for (Path p = entryOutputDir; !p.equals(outputDirPath); p = p.getParent()) {
                    newDirs.add(p);
                }
                try (InputStream in = zipFile.getInputStream(entry)) {
                    FileReference entryOutputFileRef = localIoManager.resolveAbsolutePath(entryOutputPath.toString());
                    if (logTraceEnabled) {
                        LOGGER.trace("Extracting file {}", entryOutputFileRef);
                    }
                    totalSize += writeAndForce(entryOutputFileRef, in, writeBuf, localIoManager, limited);
                }
                numEntries++;
            }
        }
        for (Path newDir : newDirs) {
            IoUtil.flushDirectory(newDir);
        }
    }

    @Override
    public long writeAndForce(FileReference outputFile, InputStream dataStream, byte[] copyBuffer,
            IIOManager localIoManager, boolean limited) throws IOException {
        long written;
        outputFile.getFile().createNewFile();
        IFileHandle fHandle = localIoManager.open(outputFile, IIOManager.FileReadWriteMode.READ_WRITE,
                IIOManager.FileSyncMode.METADATA_ASYNC_DATA_ASYNC);
        WritableByteChannel outChannel = localIoManager.newWritableChannel(fHandle);
        try (OutputStream outputStream = Channels.newOutputStream(outChannel)) {
            InputStream limitedStream = ByteStreams.limit(dataStream, maxFileSize);
            written = ByteStreams.copy(limitedStream, outputStream);
            //Check if after writing the limited stream, there's still data to be written from the entry
            if (limited && dataStream.available() > 0) {
                throw new IOException("Library contains file exceeding maximum configured allowable size");
            }
            outputStream.flush();
            localIoManager.sync(fHandle, true);
            return written;
        } finally {
            localIoManager.close(fHandle);
        }
    }

    public CloseableHttpClient newClient() {
        if (sslEnabled) {
            return uploadClientSupp.apply(this);
        } else {
            return HttpClients.createDefault();
        }
    }

    @Override
    public void setUploadClient(Function<ILibraryManager, CloseableHttpClient> f) {
        uploadClientSupp = f;
    }

    private static CloseableHttpClient defaultHttpClient(ILibraryManager extLib) {
        try {
            final INetworkSecurityManager networkSecurityManager = extLib.getNcs().getNetworkSecurityManager();
            final INetworkSecurityConfig configuration = networkSecurityManager.getConfiguration();
            KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
            try (FileInputStream trustStoreFile = new FileInputStream(configuration.getTrustStoreFile())) {
                Optional<char[]> ksPassword = configuration.getKeyStorePassword();
                trustStore.load(trustStoreFile, ksPassword.orElse(null));
            }
            SSLContext sslcontext = NetworkSecurityManager.newSSLContext(configuration, false);
            SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(sslcontext, new String[] { "TLSv1.2" },
                    null, SSLConnectionSocketFactory.getDefaultHostnameVerifier());
            return HttpClients.custom().setSSLSocketFactory(sslsf).build();

        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void writeShim(FileReference outputFile, byte[] copyBuf) throws IOException {
        InputStream is = getClass().getClassLoader().getResourceAsStream(outputFile.getFile().getName());
        if (is == null) {
            throw new IOException("Classpath does not contain necessary Python resources!");
        }
        try {
            if (ncs.getConfiguration().isCloudDeployment()) {
                writeAndForce(outputFile, is, copyBuf, ((ICloudIOManager) ioManager).getLocalIOManager(), true);
            } else {
                writeAndForce(outputFile, is, copyBuf, ioManager, true);
            }
        } finally {
            is.close();
        }
    }

    @Override
    public IIOManager getCloudIOManager() {
        return ioManager;
    }

    public static void writeDescriptor(ILibraryManager libraryManager, FileReference descFile, LibraryDescriptor desc,
            boolean cloud, byte[] copyBuf) throws IOException {
        byte[] bytes = libraryManager.serializeLibraryDescriptor(desc);
        libraryManager.writeAndForce(descFile, new ByteArrayInputStream(bytes), copyBuf,
                libraryManager.getCloudIOManager(), true);
    }

}
