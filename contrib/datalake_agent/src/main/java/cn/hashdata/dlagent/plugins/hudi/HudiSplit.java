/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.hashdata.dlagent.plugins.hudi;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class HudiSplit {
    private final String instantTime;
    private final Optional<HudiFile> baseFile;
    private final List<HudiFile> logFiles;
    private final long sizeInBytes;

    public HudiSplit(String instantTime, Optional<HudiFile> baseFile, List<HudiFile> logFiles, long sizeInBytes) {
        this.instantTime = requireNonNull(instantTime, "instantTime is null");
        this.baseFile = requireNonNull(baseFile, "baseFile is null");
        this.logFiles = requireNonNull(logFiles, "logFiles is null");
        this.sizeInBytes = sizeInBytes;
    }

    public String getInstantTime()
    {
        return instantTime;
    }

    public Optional<HudiFile> getBaseFile()
    {
        return baseFile;
    }

    public List<HudiFile> getLogFiles()
    {
        return logFiles;
    }

    public long getSplitSize() { return sizeInBytes; }

    public String toString()
    {
        return toStringHelper(this)
                .add("baseFile", baseFile)
                .add("logFiles", logFiles)
                .toString();
    }
}
