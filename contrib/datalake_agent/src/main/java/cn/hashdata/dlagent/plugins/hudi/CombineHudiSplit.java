package cn.hashdata.dlagent.plugins.hudi;

import java.util.List;

public class CombineHudiSplit {
    private final List<HudiSplit> combineSplits;

    public CombineHudiSplit(List<HudiSplit> combineSplits) {
        this.combineSplits = combineSplits;
    }

    public List<HudiSplit> getCombineSplit() {
        return combineSplits;
    }
}
