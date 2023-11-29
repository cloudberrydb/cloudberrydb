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

import org.apache.hudi.common.model.HoodieFileFormat;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.parquet.Strings.isNullOrEmpty;

public class HudiFile
{
    private final String path;
    private final long start;
    private final long length;
    private final HoodieFileFormat fileFormat;

    public HudiFile(String path, long start, long length, HoodieFileFormat fileFormat) {
        checkArgument(!isNullOrEmpty(path), "path is null or empty");
        checkArgument(start >= 0, "start is negative");
        checkArgument(length >= 0, "length is negative");

        this.path = path;
        this.start = start;
        this.length = length;
        this.fileFormat = fileFormat;
    }

    public String getPath()
    {
        return path;
    }

    public long getStart()
    {
        return start;
    }

    public long getLength()
    {
        return length;
    }

    public HoodieFileFormat getFileFormat() { return fileFormat; }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        HudiFile hudiFile = (HudiFile) o;
        return start == hudiFile.start && length == hudiFile.length && path.equals(hudiFile.path);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(path, start, length);
    }

    @Override
    public String toString()
    {
        return path + ":" + start + "+" + length;
    }
}
