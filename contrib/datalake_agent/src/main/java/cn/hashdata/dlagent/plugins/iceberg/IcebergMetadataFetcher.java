package cn.hashdata.dlagent.plugins.iceberg;

import cn.hashdata.dlagent.api.error.UnsupportedTypeException;
import cn.hashdata.dlagent.api.filter.Operator;
import cn.hashdata.dlagent.api.filter.TreeTraverser;
import cn.hashdata.dlagent.api.filter.FilterParser;
import cn.hashdata.dlagent.api.filter.Node;
import cn.hashdata.dlagent.api.filter.SupportedDataTypePruner;
import cn.hashdata.dlagent.api.filter.SupportedOperatorPruner;
import cn.hashdata.dlagent.api.io.DataType;
import cn.hashdata.dlagent.api.model.BasePlugin;
import cn.hashdata.dlagent.api.model.MetadataFetcher;
import cn.hashdata.dlagent.api.model.ScanTask;
import cn.hashdata.dlagent.api.model.CombinedTask;
import cn.hashdata.dlagent.api.model.Partition;
import cn.hashdata.dlagent.api.model.Metadata;
import cn.hashdata.dlagent.api.model.Fragment;
import cn.hashdata.dlagent.api.model.FragmentDescription;
import cn.hashdata.dlagent.api.utilities.ColumnDescriptor;
import cn.hashdata.dlagent.api.utilities.SpringContext;
import cn.hashdata.dlagent.api.utilities.Utilities;
import cn.hashdata.dlagent.plugins.hudi.utilities.FilePathUtils;
import cn.hashdata.dlagent.plugins.iceberg.utilities.IcebergUtilities;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.FileContent.EQUALITY_DELETES;

public class IcebergMetadataFetcher extends BasePlugin implements MetadataFetcher {
    private static final Logger LOG = LoggerFactory.getLogger(IcebergMetadataFetcher.class);

    // ----- members for predicate pushdown handling -----
    static final EnumSet<Operator> SUPPORTED_OPERATORS =
            EnumSet.of(
                    Operator.NOOP,
                    Operator.LESS_THAN,
                    Operator.GREATER_THAN,
                    Operator.LESS_THAN_OR_EQUAL,
                    Operator.GREATER_THAN_OR_EQUAL,
                    Operator.EQUALS,
                    Operator.NOT_EQUALS,
                    // Operator.LIKE,
                    Operator.IS_NULL,
                    Operator.IS_NOT_NULL,
                    Operator.IN,
                    Operator.OR,
                    Operator.AND,
                    Operator.NOT
            );

    static final EnumSet<DataType> SUPPORTED_DATATYPES =
            EnumSet.of(
                    DataType.BIGINT,
                    DataType.INTEGER,
                    DataType.SMALLINT,
                    DataType.REAL,
                    DataType.NUMERIC,
                    DataType.FLOAT8,
                    DataType.TEXT,
                    DataType.VARCHAR,
                    DataType.BPCHAR,
                    DataType.BOOLEAN,
                    DataType.DATE,
                    DataType.TIMESTAMP,
                    DataType.TIMESTAMP_WITH_TIME_ZONE,
                    DataType.TIME,
                    DataType.BYTEA
            );

    private final IcebergCatalogWrapper icebergClientWrapper;
    protected final IcebergUtilities icebergUtilities;

    public IcebergMetadataFetcher() {
        this(SpringContext.getBean(IcebergUtilities.class), SpringContext.getBean(IcebergCatalogWrapper.class));
    }

    IcebergMetadataFetcher(IcebergUtilities icebergUtilities, IcebergCatalogWrapper icebergClientWrapper) {
        this.icebergUtilities = icebergUtilities;
        this.icebergClientWrapper = icebergClientWrapper;
    }

    @Override
    public void afterPropertiesSet() {
        super.afterPropertiesSet();
    }

    public FragmentDescription getFragments(String pattern) throws Exception {
        IcebergCatalog catalog = icebergClientWrapper.getIcebergCatalog(context);
        Table table = catalog.loadTable(context.getDataSource());

        icebergUtilities.verifySchema(table.schema(), context);
        
        TableScan scan = table
                .newScan()
                .project(expectedSchema(table));

        Expression expression = filterExpression();
        if (expression != null) {
            scan = scan.filter(expression);
        }

        scan = scan.option(TableProperties.SPLIT_SIZE, String.valueOf(context.getSplitSize()));

        List<CombinedScanTask> scanTasks;
        try (CloseableIterable<CombinedScanTask> tasksIterable = scan.planTasks()) {
            scanTasks = Lists.newArrayList(tasksIterable);
        } catch (IOException e) {
            throw e;
        }

        return new FragmentDescription(null, transformTasks(table, scanTasks));
    }

    public List<Partition> getPartitions(String pattern) throws Exception {
        throw new UnsupportedOperationException("Iceberg accessor does not support getPartitions operation.");
    }

    public Metadata getSchema(String pattern) throws Exception {
        Metadata.Item tblDesc = Utilities.extractTableFromName(context.getDataSource());
        Metadata metadata = new Metadata(tblDesc);

        IcebergCatalog catalog = icebergClientWrapper.getIcebergCatalog(context);
        Table table = catalog.loadTable(context.getDataSource());

        LOG.info("table properties {}", table.properties());

        try {
            for (Types.NestedField icebergCol : table.schema().columns()) {
                metadata.addField(icebergUtilities.mapIcebergType(icebergCol));
            }
        } catch (UnsupportedTypeException e) {
            String errorMsg = "Failed to retrieve metadata for table " + metadata.getItem() + ". " +
                    e.getMessage();
            throw new UnsupportedTypeException(errorMsg);
        }

        return metadata;
    }

    public Schema expectedSchema(Table table) {
        Map<String, Types.NestedField> columnMetadata = Maps.newHashMap();

        table.schema().columns().stream()
                .forEach(column -> columnMetadata.put(column.name(), column));

        List<Types.NestedField> projectedFields = context.getTupleDescription().stream()
                .filter(ColumnDescriptor::isProjected)
                .map(c -> {
                    Types.NestedField t = columnMetadata.get(c.columnName());
                    if (t == null) {
                        throw new IllegalArgumentException(
                                String.format("Column %s is missing from iceberg schema", c.columnName()));
                    }
                    return t;
                })
                .collect(Collectors.toList());
        return new Schema(projectedFields);
    }

    protected EnumSet<DataType> getSupportedDatatypesForPushdown() {
        return SUPPORTED_DATATYPES;
    }

    protected EnumSet<Operator> getSupportedOperatorsForPushdown() {
        return SUPPORTED_OPERATORS;
    }

    protected Expression filterExpression() throws Exception {
        if (!context.hasFilter()) {
            return null;
        }

        /* Predicate push-down configuration */
        IcebergExpressionBuilder expressionBuilder =
                new IcebergExpressionBuilder(context.getTupleDescription());

        // Parse the filter string into a expression tree Node
        Node root = new FilterParser().parse(FilePathUtils.unescapePathName(context.getFilterString()));
        TreeTraverser traverser = new TreeTraverser();

        // Prune the parsed tree with valid supported datatypes and operators and then
        // traverse the pruned tree with the expressionBuilder to produce a Expression
        traverser.traverse(
                root,
                new SupportedDataTypePruner(context.getTupleDescription(), getSupportedDatatypesForPushdown()),
                new SupportedOperatorPruner(getSupportedOperatorsForPushdown()),
                expressionBuilder);

        Expression expression = expressionBuilder.build();
        if (expression == null) {
            return null;
        }

        LOG.debug("filter expression {}", expression.toString());

        return expression;
    }

    private List<String> getEqColumnNames(Table table, DeleteFile delete) {
        Set<Integer> deleteIds = Sets.newHashSet(delete.equalityFieldIds());
        Schema deleteSchema = TypeUtil.select(table.schema(), deleteIds);

        List<String> eqColumnNames = deleteSchema.columns().stream()
                .map(Types.NestedField::name)
                .collect(Collectors.toList());

        return eqColumnNames;
    }

    private List<CombinedTask> transformTasks(Table table, List<CombinedScanTask> scanTasks) {
        List<CombinedTask> tasks = Lists.newArrayList();

        for (CombinedScanTask combinedScanTask : scanTasks) {
            List<ScanTask> combinedTask = Lists.newArrayList();
            for (FileScanTask fileScanTask : combinedScanTask.tasks()) {
                if (fileScanTask.isDataTask()) {
                    continue;
                }

                DataFile file = fileScanTask.file();

                Fragment data = new Fragment(file.path().toString(),
                        new IcebergFileFragmentMetadata(file.format(), file.content(), file.recordCount(), null));

                List<Fragment> deletes = Lists.newArrayList();
                for (DeleteFile delete : fileScanTask.deletes()) {
                    List<String> deleteSchemas = null;
                    if (delete.content() == EQUALITY_DELETES) {
                        deleteSchemas = getEqColumnNames(table, delete);
                    }

                    Fragment deleteFragment = new Fragment(delete.path().toString(),
                            new IcebergFileFragmentMetadata(delete.format(), delete.content(), delete.recordCount(), deleteSchemas));
                    deletes.add(deleteFragment);
                }

                combinedTask.add(new ScanTask(data, deletes, fileScanTask.start(), fileScanTask.length(), null));
            }

            tasks.add(new CombinedTask(combinedTask));
        }

        return tasks;
    }

    public boolean open() throws Exception {
        return true;
    }

    public void close() throws Exception {
    }
}
