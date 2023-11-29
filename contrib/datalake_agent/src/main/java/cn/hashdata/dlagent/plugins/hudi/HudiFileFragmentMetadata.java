package cn.hashdata.dlagent.plugins.hudi;

import cn.hashdata.dlagent.api.utilities.FragmentMetadata;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
@Getter
@Setter
public class HudiFileFragmentMetadata implements FragmentMetadata {

    private String fileContent;
    private String fileFormat;

    public HudiFileFragmentMetadata(String format, String content) {
        fileFormat = format;
        fileContent = content;
    }
}
