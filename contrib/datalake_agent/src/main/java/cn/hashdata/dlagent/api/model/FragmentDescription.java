package cn.hashdata.dlagent.api.model;

import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
public class FragmentDescription {
    private TableOptions tableOptions;
    private List<CombinedTask> combinedTasks;

    public FragmentDescription(TableOptions tableOptions, List<CombinedTask> combinedTasks) {
        this.tableOptions = tableOptions;
        this.combinedTasks = combinedTasks;
    }
}
