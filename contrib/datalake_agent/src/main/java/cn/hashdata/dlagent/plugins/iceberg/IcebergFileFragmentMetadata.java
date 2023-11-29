package cn.hashdata.dlagent.plugins.iceberg;

import cn.hashdata.dlagent.api.utilities.FragmentMetadata;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;

import java.util.List;

@NoArgsConstructor
public class IcebergFileFragmentMetadata implements FragmentMetadata {
    @Getter
    @Setter
    private FileContent fileContent;

    @Getter
    @Setter
    private FileFormat fileFormat;

    @Getter
    @Setter
    private Long recordCount;

    @Getter
    @Setter
    private List<String> eqColumnNames;

    public IcebergFileFragmentMetadata(FileFormat format, FileContent content, Long recordCount, List<String> eqColumnNames) {
        fileFormat = format;
        fileContent = content;
        this.recordCount = recordCount;
        this.eqColumnNames = eqColumnNames;
    }
}
