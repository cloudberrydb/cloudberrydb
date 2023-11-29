package cn.hashdata.dlagent.api.model;

import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
public class TableOptions {
    protected boolean isTablePartitioned;
    protected String[] recordKeyFields;
    protected String[] partitionKeyFields;
    protected String preCombineField;
    protected String recordMergerStrategy;
    protected List<String> completedInstants;
    protected List<String> inflightInstants;
    protected String firstNonSavepointCommit;
    protected boolean extractPartitionValueFromPath;
    protected boolean hiveStylePartitioningEnabled;
    protected boolean isMorTable;
}
