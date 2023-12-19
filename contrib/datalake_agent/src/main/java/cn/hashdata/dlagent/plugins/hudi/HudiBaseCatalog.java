package cn.hashdata.dlagent.plugins.hudi;

import cn.hashdata.dlagent.api.filter.TreeVisitor;
import cn.hashdata.dlagent.api.filter.SupportedOperatorPruner;
import cn.hashdata.dlagent.api.filter.SupportedDataTypePruner;
import cn.hashdata.dlagent.api.filter.TreeTraverser;
import cn.hashdata.dlagent.api.filter.FilterParser;
import cn.hashdata.dlagent.api.model.Metadata;
import cn.hashdata.dlagent.api.model.RequestContext;
import cn.hashdata.dlagent.api.security.SecureLogin;
import cn.hashdata.dlagent.api.utilities.ColumnDescriptor;
import cn.hashdata.dlagent.plugins.hudi.utilities.DataTypeUtils;
import cn.hashdata.dlagent.plugins.hudi.utilities.FilePathUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter;
import org.apache.hudi.internal.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;
import java.util.stream.Collectors;

public abstract class HudiBaseCatalog {
    private static final Logger LOG = LoggerFactory.getLogger(HudiBaseCatalog.class);

    protected final HoodieTableMetaClient metaClient;
    protected final HoodieTableConfig tableConfig;
    protected final String[] partitionColumns;

    private DataPruner dataPruner;
    private HudiPartitionPruner.PartitionPruner partitionPruner;
    protected HudiFileIndex fileIndex;
    protected SecureLogin secureLogin;

    public HudiBaseCatalog(HoodieTableMetaClient metadataClient, SecureLogin secureLogin) {
        metaClient = metadataClient;
        tableConfig = metaClient.getTableConfig();
        partitionColumns = tableConfig.getPartitionFields().orElse(new String[0]);
        this.secureLogin = secureLogin;
    }

    protected InternalSchema getTableSchema() throws Exception {
        TableSchemaResolver schemaResolver = new TableSchemaResolver(metaClient);
        return AvroInternalSchemaConverter.convert(schemaResolver.getTableAvroSchema(true));
    }

    @Nullable
    private HudiPartitionPruner.PartitionPruner cratePartitionPruner(RequestContext context,
                                                                     ExpressionEvaluators.Evaluator evaluator) {
        if (evaluator == null) {
            return null;
        }

        List<Type.TypeID> partitionTypes = Arrays.stream(partitionColumns).map(
                    name -> context.getColumn(name).orElseThrow(() ->
                            new HoodieValidationException("field " + name + " does not exist"))
                )
                .map(c -> DataTypeUtils.mapHudiType(c.getDataType()))
                .collect(Collectors.toList());

        String defaultParName = context.getDefaultPartitionName();
        boolean hivePartition = Boolean.parseBoolean(tableConfig.getHiveStylePartitioningEnable());

        return HudiPartitionPruner.getInstance(
                evaluator, Arrays.asList(partitionColumns), partitionTypes, defaultParName, hivePartition);
    }

    private Pair<ExpressionEvaluators.Evaluator, Set<String>> createFilter(RequestContext context,
                                                        TreeVisitor operatorPruner) throws Exception {
        List<ColumnDescriptor> columns = context.getTupleDescription();
        ExpressionEvaluators builder = new ExpressionEvaluators(columns);
        TreeVisitor dataTypePruner = new SupportedDataTypePruner(columns,
                HudiMetadataFetcher.getSupportedDatatypesForPushdown());

        TreeTraverser traverser = new TreeTraverser();
        traverser.traverse(new FilterParser().parse(FilePathUtils.unescapePathName(context.getFilterString())),
                dataTypePruner,
                operatorPruner,
                builder);

        return builder.build();
    }

    private Pair<Pair<ExpressionEvaluators.Evaluator, Set<String>>,
            ExpressionEvaluators.Evaluator> splitExprByPartitionKeys(RequestContext context) throws Exception {
        if (!context.hasFilter()) {
            return Pair.of(null, null);
        }

        if (partitionColumns.length == 0) {
            return Pair.of(createFilter(context,
                    new SupportedOperatorPruner(HudiMetadataFetcher.getSupportedOperatorsForPushdown())), null);
        }

        Set<String> partitionKeys = Arrays.stream(partitionColumns).collect(Collectors.toSet());

        TreeVisitor partitionPruner = new ColumnPruner(
                HudiMetadataFetcher.getSupportedOperatorsForPushdown(),
                partitionKeys,
                context.getTupleDescription(),
                true);

        Pair<ExpressionEvaluators.Evaluator, Set<String>> partitionFilter = createFilter(context, partitionPruner);

        TreeVisitor dataPruner = new ColumnPruner(
                HudiMetadataFetcher.getSupportedOperatorsForPushdown(),
                partitionKeys,
                context.getTupleDescription(),
                false);
        Pair<ExpressionEvaluators.Evaluator, Set<String>> dataFilter = createFilter(context, dataPruner);

        return Pair.of(dataFilter, partitionFilter != null ? partitionFilter.getLeft() : null);
    }

    private HudiFileIndex getOrBuildFileIndex(Metadata.Item tableName, RequestContext context) throws Exception {
        if (fileIndex == null) {
            Pair<Pair<ExpressionEvaluators.Evaluator, Set<String>>, ExpressionEvaluators.Evaluator>
                    splitFilters = splitExprByPartitionKeys(context);

            Pair<ExpressionEvaluators.Evaluator, Set<String>> dataFilter = splitFilters.getLeft();
            if (dataFilter != null) {
                dataPruner = DataPruner.newInstance(dataFilter.getLeft(),
                        dataFilter.getRight().toArray(new String[0]));
            }

            partitionPruner = cratePartitionPruner(context, splitFilters.getRight());

            fileIndex = createFileIndex(partitionPruner, dataPruner, context, tableName);
        }
        return fileIndex;
    }

    public abstract HudiFileIndex createFileIndex(HudiPartitionPruner.PartitionPruner partitionPruner,
                                                  DataPruner dataPruner,
                                                  RequestContext context,
                                                  Metadata.Item tableName) throws Exception;

    private List<HudiSplit> buildSnapshotInputSplits(Metadata.Item tableName, RequestContext context) throws Exception {
        HudiFileIndex fileIndex = getOrBuildFileIndex(tableName, context);
        List<String> relPartitionPaths = fileIndex.getOrBuildPartitionPaths();
        if (relPartitionPaths.size() == 0) {
            return Collections.emptyList();
        }

        FileStatus[] fileStatuses = fileIndex.getFilesInPartitions();
        if (fileStatuses.length == 0) {
            return Collections.emptyList();
        }

        HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metaClient,
                // file-slice after pending compaction-requested instant-time is also considered valid
                metaClient.getCommitsAndCompactionTimeline().filterCompletedAndCompactionInstants(), fileStatuses);

        String latestCommit = fsView.getLastInstant().get().getTimestamp();
        HoodieFileFormat fileFormat = tableConfig.getBaseFileFormat();

        // generates one input split for each file group
        return relPartitionPaths.stream()
                .map(relPartitionPath -> fsView.getLatestMergedFileSlicesBeforeOrOn(relPartitionPath, latestCommit)
                        .map(fileSlice -> {
                            HudiFile baseFile = fileSlice.getBaseFile().map(
                                    f -> new HudiFile(f.getPath(), 0, f.getFileLen(), fileFormat)).orElse(null);
                            List<HudiFile> logFiles = fileSlice.getLogFiles()
                                    .sorted(HoodieLogFile.getLogFileComparator())
                                    .map(l -> new HudiFile(l.getPath().toString(), 0, l.getFileSize(), HoodieFileFormat.HOODIE_LOG))
                                    .collect(Collectors.toList());

                            long logFilesSize = logFiles.size() > 0 ? logFiles.stream()
                                    .map(HudiFile::getLength)
                                    .reduce(0L, Long::sum) : 0L;
                            long sizeInBytes = baseFile != null ? baseFile.getLength() + logFilesSize : logFilesSize;

                            return new HudiSplit(latestCommit, Optional.ofNullable(baseFile), logFiles, sizeInBytes);
                        }).collect(Collectors.toList()))
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    private List<HudiSplit> buildBaseFileInputSplits(Metadata.Item tableName, RequestContext context) throws Exception {
        HudiFileIndex fileIndex = getOrBuildFileIndex(tableName, context);
        List<String> relPartitionPaths = fileIndex.getOrBuildPartitionPaths();
        if (relPartitionPaths.size() == 0) {
            return Collections.emptyList();
        }

        FileStatus[] fileStatuses = fileIndex.getFilesInPartitions();
        if (fileStatuses.length == 0) {
            return Collections.emptyList();
        }

        HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metaClient,
                metaClient.getCommitsAndCompactionTimeline().filterCompletedInstants(), fileStatuses);

        String latestCommit = fsView.getLastInstant().get().getTimestamp();
        HoodieFileFormat fileFormat = tableConfig.getBaseFileFormat();

        return fsView.getLatestBaseFiles()
                .map(f -> {
                    HudiFile baseFile = new HudiFile(f.getPath().toString(), 0, f.getFileSize(), fileFormat);
                    return new HudiSplit(latestCommit,
                            Optional.ofNullable(baseFile),
                            Collections.emptyList(),
                            baseFile.getLength());
                })
                .collect(Collectors.toList());
    }

    private List<HudiSplit> buildIncrementalSplits(Metadata.Item tableName, RequestContext context) throws Exception {
        throw new UnsupportedOperationException("Hudi accessor does not support incremental query.");
    }

    private HudiTableOptions resolveTableOptions() {
        HudiTableOptions options = new HudiTableOptions();

        options.setTablePartitioned(tableConfig.isTablePartitioned());
        options.setRecordKeyFields(tableConfig.getRecordKeyFields().orElse(null));
        options.setPartitionKeyFields(tableConfig.getPartitionFields().orElse(null));
        options.setPreCombineField(tableConfig.getPreCombineField());
        options.setRecordMergerStrategy(tableConfig.getRecordMergerStrategy());

        HoodieTimeline commitsTimeline = metaClient.getCommitsTimeline();
        HoodieTimeline completedTimeline = commitsTimeline.filterCompletedInstants();
        HoodieTimeline inflightTimeline = commitsTimeline.filterInflights();
        options.setCompletedInstants(completedTimeline.getInstantsAsStream()
                .map(s -> s.getTimestamp()).collect(Collectors.toList()));
        options.setInflightInstants(inflightTimeline.getInstantsAsStream()
                .map(s -> s.getTimestamp()).collect(Collectors.toList()));
        options.setFirstNonSavepointCommit(completedTimeline.getFirstNonSavepointCommit()
                .map(HoodieInstant::getTimestamp).orElse(null));

        options.setExtractPartitionValueFromPath(tableConfig.shouldDropPartitionColumns());
        options.setHiveStylePartitioningEnabled(Boolean.parseBoolean(tableConfig.getHiveStylePartitioningEnable()));
        options.setMorTable(tableConfig.getTableType() == HoodieTableType.MERGE_ON_READ);

        return options;
    }

    protected Pair<HudiTableOptions, List<CombineHudiSplit>> groupSplits(List<HudiSplit> splits, long splitSize) throws Exception {
        List<CombineHudiSplit> result = new ArrayList<>();

        long curSplitSize = 0;
        List<HudiSplit> curCombineSplit = null;
        for (HudiSplit split : splits) {
            if (split.getSplitSize() > splitSize) {
                result.add(new CombineHudiSplit(Collections.singletonList(split)));
                continue;
            }

            if (curCombineSplit == null) {
                curCombineSplit = new ArrayList<>();
            }

            curCombineSplit.add(split);
            curSplitSize += split.getSplitSize();

            if (curSplitSize > splitSize) {
                result.add(new CombineHudiSplit(curCombineSplit));
                curSplitSize = 0;
                curCombineSplit = null;
            }
        }

        if (curCombineSplit != null && !curCombineSplit.isEmpty()) {
            result.add(new CombineHudiSplit(curCombineSplit));
        }

        return Pair.of(resolveTableOptions(), result);
    }

    protected Pair<HudiTableOptions, List<CombineHudiSplit>> buildInputSplits (Metadata.Item tableName,
                                                                               RequestContext context) throws Exception {
        if (tableConfig.getTableType() == HoodieTableType.MERGE_ON_READ) {
            switch (context.getQueryType()) {
                case "snapshot":
                    return groupSplits(buildSnapshotInputSplits(tableName, context), context.getSplitSize());
                case "readoptimized":
                    return groupSplits(buildBaseFileInputSplits(tableName, context), context.getSplitSize());
                default:
                    return groupSplits(buildIncrementalSplits(tableName, context), context.getSplitSize());
            }
        }

        if (context.getQueryType().equals("snapshot")) {
            return groupSplits(buildBaseFileInputSplits(tableName, context), context.getSplitSize());
        }

        return groupSplits(buildIncrementalSplits(tableName, context), context.getSplitSize());
    }
}
