package cn.hashdata.dlagent.plugins.hudi;

import cn.hashdata.dlagent.api.model.RequestContext;
import cn.hashdata.dlagent.api.security.SecureLogin;
import cn.hashdata.dlagent.api.utilities.Utilities;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hudi.avro.model.HoodieMetadataColumnStats;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.Collection;
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class HudiFsBackedTableMetadata implements HoodieTableMetadata {

    private static final Logger LOG = LoggerFactory.getLogger(HudiFsBackedTableMetadata.class);

    private static final int DEFAULT_LISTING_PARALLELISM = 12;
    private final transient HoodieEngineContext engineContext;
    private final SerializableConfiguration hadoopConf;
    private final String datasetBasePath;
    private final boolean assumeDatePartitioning;
    private final SecureLogin secureLogin;
    private final RequestContext requestContext;

    public HudiFsBackedTableMetadata(HoodieEngineContext engineContext,
                                     SerializableConfiguration conf,
                                     String datasetBasePath,
                                     boolean assumeDatePartitioning,
                                     SecureLogin secureLogin,
                                     RequestContext requestContext) {
        this.engineContext = engineContext;
        this.hadoopConf = conf;
        this.datasetBasePath = datasetBasePath;
        this.assumeDatePartitioning = assumeDatePartitioning;
        this.secureLogin = secureLogin;
        this.requestContext = requestContext;
    }

    @Override
    public FileStatus[] getAllFilesInPartition(Path partitionPath) throws IOException {
        FileSystem fs = partitionPath.getFileSystem(hadoopConf.get());
        return FSUtils.getAllDataFilesInPartition(fs, partitionPath);
    }

    @Override
    public List<String> getAllPartitionPaths() throws IOException {
        Path basePath = new Path(datasetBasePath);
        FileSystem fs = basePath.getFileSystem(hadoopConf.get());
        if (assumeDatePartitioning) {
            return FSUtils.getAllPartitionFoldersThreeLevelsDown(fs, datasetBasePath);
        }

        return getPartitionPathWithPathPrefixes(Collections.singletonList(""));
    }

    @Override
    public List<String> getPartitionPathWithPathPrefixes(List<String> relativePathPrefixes) throws IOException {
        return relativePathPrefixes.stream().flatMap(relativePathPrefix -> {
            try {
                return getPartitionPathWithPathPrefix(relativePathPrefix).stream();
            } catch (Exception e) {
                throw new HoodieException("Error fetching partition paths with relative path: " + relativePathPrefix, e);
            }
        }).collect(Collectors.toList());
    }

    private List<FileStatus> listDirectory(List<Path> pathsToList, int listingParallelism) throws Exception {
        if (Utilities.isSecurityEnabled(hadoopConf.get())) {
            UserGroupInformation loginUser = secureLogin.getLoginUser(requestContext.getServerName(),
                    requestContext.getConfig(), hadoopConf.get());

            // wrap in doAs for Kerberos to propagate kerberos tokens from login Subject
            return engineContext.flatMap(pathsToList, path -> {
                return loginUser.doAs((PrivilegedExceptionAction<Stream<FileStatus>>) () ->
                        Arrays.stream(path.getFileSystem(hadoopConf.get()).listStatus(path)));
            }, listingParallelism);

        } else {
            return engineContext.flatMap(pathsToList, path -> {
                FileSystem fileSystem = path.getFileSystem(hadoopConf.get());
                return Arrays.stream(fileSystem.listStatus(path));
            }, listingParallelism);
        }
    }

    private Pair<Option<String>, Option<Path>> processFile(FileStatus fileStatus) throws Exception {
        FileSystem fileSystem = fileStatus.getPath().getFileSystem(hadoopConf.get());

        if (fileStatus.isDirectory()) {
            if (HoodiePartitionMetadata.hasPartitionMetadata(fileSystem, fileStatus.getPath())) {
                return Pair.of(Option.of(FSUtils.getRelativePartitionPath(new Path(datasetBasePath), fileStatus.getPath())),
                        Option.empty());
            } else if (!fileStatus.getPath().getName().equals(HoodieTableMetaClient.METAFOLDER_NAME)) {
                return Pair.of(Option.empty(), Option.of(fileStatus.getPath()));
            }
        } else if (fileStatus.getPath().getName().startsWith(HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE_PREFIX)) {
            String partitionName = FSUtils.getRelativePartitionPath(new Path(datasetBasePath),
                    fileStatus.getPath().getParent());
            return Pair.of(Option.of(partitionName), Option.empty());
        }

        return Pair.of(Option.empty(), Option.empty());
    }

    private List<Pair<Option<String>, Option<Path>>> processDirectory(List<FileStatus> dirToFileListing,
                                                                      int fileListingParallelism) throws Exception{
        if (Utilities.isSecurityEnabled(hadoopConf.get())) {
            UserGroupInformation loginUser = secureLogin.getLoginUser(requestContext.getServerName(),
                    requestContext.getConfig(), hadoopConf.get());

            return engineContext.map(dirToFileListing, fileStatus -> {
                return loginUser.doAs((PrivilegedExceptionAction<Pair<Option<String>, Option<Path>>>) () ->
                        processFile(fileStatus));
            }, fileListingParallelism);
        } else {
            return engineContext.map(dirToFileListing, fileStatus -> {return processFile(fileStatus);},
                    fileListingParallelism);
        }
    }

    private List<String> getPartitionPathWithPathPrefix(String relativePathPrefix) throws Exception {
        List<Path> pathsToList = new CopyOnWriteArrayList<>();
        pathsToList.add(StringUtils.isNullOrEmpty(relativePathPrefix)
                ? new Path(datasetBasePath) : new Path(datasetBasePath, relativePathPrefix));
        List<String> partitionPaths = new CopyOnWriteArrayList<>();

        while (!pathsToList.isEmpty()) {
            int listingParallelism = Math.min(DEFAULT_LISTING_PARALLELISM, pathsToList.size());

            // List all directories in parallel
            List<FileStatus> dirToFileListing = listDirectory(pathsToList, listingParallelism);

            pathsToList.clear();

            // if current dictionary contains PartitionMetadata, add it to result
            // if current dictionary does not contain PartitionMetadata, add it to queue to be processed.
            int fileListingParallelism = Math.min(DEFAULT_LISTING_PARALLELISM, dirToFileListing.size());
            if (!dirToFileListing.isEmpty()) {
                // result below holds a list of pair. first entry in the pair optionally holds the deduced list of partitions.
                // and second entry holds optionally a directory path to be processed further.
                List<Pair<Option<String>, Option<Path>>> result = processDirectory(dirToFileListing, fileListingParallelism);

                partitionPaths.addAll(result.stream().filter(entry -> entry.getKey().isPresent()).map(entry -> entry.getKey().get())
                        .collect(Collectors.toList()));

                pathsToList.addAll(result.stream().filter(entry -> entry.getValue().isPresent()).map(entry -> entry.getValue().get())
                        .collect(Collectors.toList()));
            }
        }
        return partitionPaths;
    }

    @Override
    public Map<String, FileStatus[]> getAllFilesInPartitions(Collection<String> partitionPaths)
            throws IOException {
        if (partitionPaths == null || partitionPaths.isEmpty()) {
            return Collections.emptyMap();
        }

        List<Pair<String, FileStatus[]>> partitionToFiles = null;
        int parallelism = Math.min(DEFAULT_LISTING_PARALLELISM, partitionPaths.size());

        if (Utilities.isSecurityEnabled(hadoopConf.get())) {
            UserGroupInformation loginUser = secureLogin.getLoginUser(requestContext.getServerName(),
                    requestContext.getConfig(), hadoopConf.get());

            try {
                partitionToFiles = engineContext.map(new ArrayList<>(partitionPaths), partitionPathStr -> {
                    Path partitionPath = new Path(partitionPathStr);
                    return loginUser.doAs((PrivilegedExceptionAction<Pair<String, FileStatus[]>>) () ->
                            Pair.of(partitionPathStr, FSUtils.getAllDataFilesInPartition(
                                    partitionPath.getFileSystem(hadoopConf.get()), partitionPath)));
                }, parallelism);

            } catch (Exception e) {
                throw new HoodieException("Error while listing directory: ", e);
            }
        } else {
            partitionToFiles = engineContext.map(new ArrayList<>(partitionPaths), partitionPathStr -> {
                Path partitionPath = new Path(partitionPathStr);
                FileSystem fs = partitionPath.getFileSystem(hadoopConf.get());
                return Pair.of(partitionPathStr, FSUtils.getAllDataFilesInPartition(fs, partitionPath));
            }, parallelism);
        }

        return partitionToFiles.stream().collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
    }

    @Override
    public Option<String> getSyncedInstantTime() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Option<String> getLatestCompactionTime() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws Exception {
        // no-op
    }

    @Override
    public void reset() {
        // no-op
    }

    public Option<BloomFilter> getBloomFilter(final String partitionName, final String fileName)
            throws HoodieMetadataException {
        throw new HoodieMetadataException("Unsupported operation: getBloomFilter for " + fileName);
    }

    @Override
    public Map<Pair<String, String>, BloomFilter> getBloomFilters(final List<Pair<String, String>> partitionNameFileNameList)
            throws HoodieMetadataException {
        throw new HoodieMetadataException("Unsupported operation: getBloomFilters!");
    }

    @Override
    public Map<Pair<String, String>, HoodieMetadataColumnStats> getColumnStats(final List<Pair<String, String>> partitionNameFileNameList, final String columnName)
            throws HoodieMetadataException {
        throw new HoodieMetadataException("Unsupported operation: getColumnsStats!");
    }

    @Override
    public HoodieData<HoodieRecord<HoodieMetadataPayload>> getRecordsByKeyPrefixes(List<String> keyPrefixes, String partitionName, boolean shouldLoadInMemory) {
        throw new HoodieMetadataException("Unsupported operation: getRecordsByKeyPrefixes!");
    }
}
