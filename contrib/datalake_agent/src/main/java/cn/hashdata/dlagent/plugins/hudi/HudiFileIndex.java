/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.hashdata.dlagent.plugins.hudi;

import cn.hashdata.dlagent.api.model.RequestContext;
import cn.hashdata.dlagent.api.security.SecureLogin;
import cn.hashdata.dlagent.plugins.hudi.data.RowData;
import cn.hashdata.dlagent.plugins.hudi.data.RowField;
import cn.hashdata.dlagent.plugins.hudi.utilities.HudiUtilities;
import cn.hashdata.dlagent.plugins.hudi.utilities.DataTypeUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.avro.model.HoodieMetadataColumnStats;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

/**
 * A file index which supports listing files efficiently through metadata table.
 *
 * <p>It caches the partition paths to avoid redundant look up.
 */
public class HudiFileIndex {
  private static final Logger LOG = LoggerFactory.getLogger(HudiFileIndex.class);

  private final Path path;
  private final boolean tableExists;
  private final HoodieMetadataConfig metadataConfig;
  private final HudiPartitionPruner.PartitionPruner partitionPruner; // for partition pruning
  private final DataPruner dataPruner;                 // for data skipping
  private List<String> partitionPaths;                 // cache of partition paths
  private final RequestContext context;
  private final SecureLogin secureLogin;

  private HudiFileIndex(Path path,
                        RequestContext context,
                        DataPruner dataPruner,
                        HudiPartitionPruner.PartitionPruner partitionPruner,
                        SecureLogin secureLogin) {
    this.path = path;
    this.tableExists = HudiUtilities.tableExists(path.toString(), context.getConfiguration());
    this.metadataConfig = getMetadataConfig(context.isMetadataTableEnabled(), context.getConfiguration());
    this.dataPruner = isDataSkippingFeasible() ? dataPruner : null;;
    this.partitionPruner = partitionPruner;
    this.context = context;
    this.secureLogin = secureLogin;
  }

  /**
   * Returns the builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns all the file statuses under the table base path.
   */
  public FileStatus[] getFilesInPartitions() {
    if (!tableExists) {
      return new FileStatus[0];
    }

    String[] partitions = getOrBuildPartitionPaths().stream().map(p -> fullPartitionPath(path, p)).toArray(String[]::new);

    Instant startTime = Instant.now();
    FileStatus[] allFiles = HudiUtilities.getFilesInPartitions(new HoodieLocalEngineContext(context.getConfiguration()),
                    metadataConfig, path.toString(), partitions, context, secureLogin)
        .values().stream()
        .flatMap(Arrays::stream)
        .toArray(FileStatus[]::new);

    Duration duration = Duration.between(startTime, Instant.now());
    LOG.info("Finished getFilesInPartitions [{}] operation in {} ms.", allFiles.length, duration.toMillis());

    if (allFiles.length == 0) {
      // returns early for empty table.
      return allFiles;
    }

    // data skipping
    Set<String> candidateFiles = candidateFilesInMetadataTable(allFiles);
    if (candidateFiles == null) {
      // no need to filter by col stats or error occurs.
      return allFiles;
    }

    FileStatus[] results = Arrays.stream(allFiles).parallel()
        .filter(fileStatus -> candidateFiles.contains(fileStatus.getPath().getName()))
        .toArray(FileStatus[]::new);

    logPruningMsg(allFiles.length, results.length, "data skipping");
    return results;
  }

  /**
   * Returns the full partition path.
   *
   * @param basePath      The base path.
   * @param partitionPath The relative partition path, may be empty if the table is non-partitioned.
   * @return The full partition path string
   */
  private static String fullPartitionPath(Path basePath, String partitionPath) {
    if (partitionPath.isEmpty()) {
      return basePath.toString();
    }

    return new Path(basePath, partitionPath).toString();
  }

  /**
   * Reset the state of the file index.
   */
  public void reset() {
    this.partitionPaths = null;
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  /**
   * Computes pruned list of candidate base-files' names based on provided list of data filters.
   * conditions, by leveraging Metadata Table's Column Statistics index (hereon referred as ColStats for brevity)
   * bearing "min", "max", "num_nulls" statistics for all columns.
   *
   * <p>NOTE: This method has to return complete set of candidate files, since only provided candidates will
   * ultimately be scanned as part of query execution. Hence, this method has to maintain the
   * invariant of conservatively including every base-file's name, that is NOT referenced in its index.
   *
   * <p>The {@code filters} must all be simple.
   *
   * @return set of pruned (data-skipped) candidate base-files' names
   */
  @Nullable
  private Set<String> candidateFilesInMetadataTable(FileStatus[] allFileStatus) {
    if (dataPruner == null) {
      return null;
    }

    try {
      String[] referencedCols = dataPruner.getReferencedCols();

      Instant startTime = Instant.now();
      final List<HoodieMetadataColumnStats> colStats = ColumnStatsIndices.readColumnStatsIndex(
              context.getConfiguration(), path.toString(), metadataConfig, referencedCols);

      final Pair<List<RowData>, String[]> colStatsTable = ColumnStatsIndices.transposeColumnStatsIndex(colStats,
              referencedCols);

      Duration duration = Duration.between(startTime, Instant.now());
      LOG.info("Finished readColumnStatsIndex operation in {} ms.", duration.toMillis());

      List<RowData> transposedColStats = colStatsTable.getLeft();
      String[] queryCols = colStatsTable.getRight();
      if (queryCols.length == 0) {
        // the indexed columns have no intersection with the referenced columns, returns early
        LOG.info("filter columns {} have no intersection with indexed columns {}", referencedCols, queryCols);
        return null;
      }

      RowField[] queryFields = DataTypeUtils.projectRowFields(context.getTupleDescription(), queryCols);
      Set<String> allIndexedFileNames = transposedColStats.stream().parallel()
          .map(row -> row.getString(0).toString())
          .collect(Collectors.toSet());

      Set<String> candidateFileNames = transposedColStats.stream().parallel()
          .filter(row -> dataPruner.test(row, queryFields))
          .map(row -> row.getString(0).toString())
          .collect(Collectors.toSet());

      // NOTE: Col-Stats Index isn't guaranteed to have complete set of statistics for every
      //       base-file: since it's bound to clustering, which could occur asynchronously
      //       at arbitrary point in time, and is not likely to be touching all the base files.
      //
      //       To close that gap, we manually compute the difference b/w all indexed (by col-stats-index)
      //       files and all outstanding base-files, and make sure that all base files not
      //       represented w/in the index are included in the output of this method
      Set<String> nonIndexedFileNames = Arrays.stream(allFileStatus)
          .map(fileStatus -> fileStatus.getPath().getName()).collect(Collectors.toSet());
      nonIndexedFileNames.removeAll(allIndexedFileNames);

      candidateFileNames.addAll(nonIndexedFileNames);
      return candidateFileNames;
    } catch (Throwable throwable) {
      LOG.warn("Read column stats for data skipping error", throwable);
      return null;
    }
  }

  /**
   * Returns all the relative partition paths.
   *
   * <p>The partition paths are cached once invoked.
   */
  public List<String> getOrBuildPartitionPaths() {
    if (partitionPaths != null) {
      return partitionPaths;
    }

    Instant startTime = Instant.now();
    List<String> allPartitionPaths = tableExists
        ? HudiUtilities.getAllPartitionPaths(new HoodieLocalEngineContext(context.getConfiguration()),
            metadataConfig, path.toString(), context, secureLogin)
        : Collections.emptyList();

    if (partitionPruner == null) {
      partitionPaths = allPartitionPaths;
    } else {
      Set<String> prunedPartitionPaths = partitionPruner.filter(allPartitionPaths);
      partitionPaths = new ArrayList<>(prunedPartitionPaths);
    }

    Duration duration = Duration.between(startTime, Instant.now());
    LOG.info("Finished getOrBuildPartitionPaths[{}] operation in {} ms.", partitionPaths.size(), duration.toMillis());
    return partitionPaths;
  }

  private HoodieMetadataConfig getMetadataConfig(boolean isMetadataSpecified, Configuration hadoopConf) {
    Boolean isMetatableEnabled = false;
    String metadataBasePath = HoodieTableMetadata.getMetadataTableBasePath(path.toString());
    Boolean metadataExists = HudiUtilities.tableExists(metadataBasePath, hadoopConf);

    if (isMetadataSpecified && metadataExists) {
        isMetatableEnabled = true;
    }

    // set up metadata.enabled=true in table DDL to enable metadata listing
    Properties properties = new Properties();
    properties.put(HoodieMetadataConfig.ENABLE.key(), isMetatableEnabled);

    return HoodieMetadataConfig.newBuilder().fromProperties(properties).build();
  }

  private boolean isDataSkippingFeasible() {
    // NOTE: Data Skipping is only effective when it references columns that are indexed w/in
    //       the Column Stats Index (CSI). Following cases could not be effectively handled by Data Skipping:
    //          - Expressions on top-level column's fields (ie, for ex filters like "struct.field > 0", since
    //          CSI only contains stats for top-level columns, in this case for "struct")
    //          - Any expression not directly referencing top-level column (for ex, sub-queries, since there's
    //          nothing CSI in particular could be applied for)
    if (metadataConfig.enabled()) {
      LOG.info("Found Metadata Table, enable the data skipping");
      return true;
    } else {
      LOG.warn("Data skipping requires Metadata Table to be enabled! Disable the data skipping");
    }

    return false;
  }

  private void logPruningMsg(int numTotalFiles, int numLeftFiles, String action) {
    LOG.info("\n"
        + "------------------------------------------------------------\n"
        + "---------- action:        {}\n"
        + "---------- total files:   {}\n"
        + "---------- left files:    {}\n"
        + "---------- skipping rate: {}\n"
        + "------------------------------------------------------------",
        action, numTotalFiles, numLeftFiles, percentage(numTotalFiles, numLeftFiles));
  }

  private static double percentage(double total, double left) {
    return (total - left) / total;
  }

  // -------------------------------------------------------------------------
  //  Inner class
  // -------------------------------------------------------------------------

  /**
   * Builder for {@link HudiFileIndex}.
   */
  public static class Builder {
    private Path path;
    private DataPruner dataPruner;
    private HudiPartitionPruner.PartitionPruner partitionPruner;
    private RequestContext context;
    private SecureLogin secureLogin;

    private Builder() {
    }

    public Builder path(Path path) {
      this.path = path;
      return this;
    }

    public Builder dataPruner(DataPruner dataPruner) {
      this.dataPruner = dataPruner;
      return this;
    }

    public Builder partitionPruner(HudiPartitionPruner.PartitionPruner partitionPruner) {
      this.partitionPruner = partitionPruner;
      return this;
    }

    public Builder context(RequestContext context) {
      this.context = context;
      return this;
    }

    public Builder secureLogin(SecureLogin secureLogin) {
      this.secureLogin = secureLogin;
      return this;
    }

    public HudiFileIndex build() {
      return new HudiFileIndex(Objects.requireNonNull(path), context, dataPruner, partitionPruner, secureLogin);
    }
  }
}
