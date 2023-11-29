/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.hashdata.dlagent.plugins.hudi.utilities;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.fs.FSUtils;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class FilePathUtils {

  private static final Pattern HIVE_PARTITION_NAME_PATTERN = Pattern.compile("([^/]+)=([^/]+)");

  private static final BitSet CHAR_TO_ESCAPE = new BitSet(128);

  static {
    for (char c = 0; c < ' '; c++) {
      CHAR_TO_ESCAPE.set(c);
    }

    /*
     * ASCII 01-1F are HTTP control characters that need to be escaped.
     * \u000A and \u000D are \n and \r, respectively.
     */
    char[] clist = new char[] {'\u0001', '\u0002', '\u0003', '\u0004',
        '\u0005', '\u0006', '\u0007', '\u0008', '\u0009', '\n', '\u000B',
        '\u000C', '\r', '\u000E', '\u000F', '\u0010', '\u0011', '\u0012',
        '\u0013', '\u0014', '\u0015', '\u0016', '\u0017', '\u0018', '\u0019',
        '\u001A', '\u001B', '\u001C', '\u001D', '\u001E', '\u001F',
        '"', '#', '%', '\'', '*', '/', ':', '=', '?', '\\', '\u007F', '{',
        '[', ']', '^'};

    for (char c : clist) {
      CHAR_TO_ESCAPE.set(c);
    }
  }

  private static boolean needsEscaping(char c) {
    return c < CHAR_TO_ESCAPE.size() && CHAR_TO_ESCAPE.get(c);
  }

  /**
   * Generates partition key value mapping from path.
   *
   * @param currPath      Partition file path
   * @param hivePartition Whether the partition path is with Hive style
   * @param partitionKeys Partition keys
   * @return Sequential partition specs.
   */
  public static LinkedHashMap<String, String> extractPartitionKeyValues(
      Path currPath,
      boolean hivePartition,
      String[] partitionKeys) {
    LinkedHashMap<String, String> fullPartSpec = new LinkedHashMap<>();
    if (partitionKeys.length == 0) {
      return fullPartSpec;
    }
    List<String[]> kvs = new ArrayList<>();
    int curDepth = 0;
    do {
      String component = currPath.getName();
      final String[] kv = new String[2];
      if (hivePartition) {
        Matcher m = HIVE_PARTITION_NAME_PATTERN.matcher(component);
        if (m.matches()) {
          String k = unescapePathName(m.group(1));
          String v = unescapePathName(m.group(2));
          kv[0] = k;
          kv[1] = v;
        }
      } else {
        kv[0] = partitionKeys[partitionKeys.length - 1 - curDepth];
        kv[1] = unescapePathName(component);
      }
      kvs.add(kv);
      currPath = currPath.getParent();
      curDepth++;
    } while (currPath != null && !currPath.getName().isEmpty() && curDepth < partitionKeys.length);

    // reverse the list since we checked the part from leaf dir to table's base dir
    for (int i = kvs.size(); i > 0; i--) {
      fullPartSpec.put(kvs.get(i - 1)[0], kvs.get(i - 1)[1]);
    }

    return fullPartSpec;
  }

  public static String unescapePathName(String path) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < path.length(); i++) {
      char c = path.charAt(i);
      if (c == '%' && i + 2 < path.length()) {
        int code = -1;
        try {
          code = Integer.parseInt(path.substring(i + 1, i + 3), 16);
        } catch (Exception ignored) {
          // do nothing
        }
        if (code >= 0) {
          sb.append((char) code);
          i += 2;
          continue;
        }
      }
      sb.append(c);
    }
    return sb.toString();
  }

  public static FileStatus[] getFileStatusRecursively(Path path, int expectLevel, Configuration conf) {
    return getFileStatusRecursively(path, expectLevel, FSUtils.getFs(path.toString(), conf));
  }

  public static FileStatus[] getFileStatusRecursively(Path path, int expectLevel, FileSystem fs) {
    ArrayList<FileStatus> result = new ArrayList<>();

    try {
      FileStatus fileStatus = fs.getFileStatus(path);
      listStatusRecursively(fs, fileStatus, 0, expectLevel, result);
    } catch (IOException ignore) {
      return new FileStatus[0];
    }

    return result.toArray(new FileStatus[0]);
  }

  private static void listStatusRecursively(
      FileSystem fs,
      FileStatus fileStatus,
      int level,
      int expectLevel,
      List<FileStatus> results) throws IOException {
    if (expectLevel == level && !isHiddenFile(fileStatus)) {
      results.add(fileStatus);
      return;
    }

    if (fileStatus.isDirectory() && !isHiddenFile(fileStatus)) {
      for (FileStatus stat : fs.listStatus(fileStatus.getPath())) {
        listStatusRecursively(fs, stat, level + 1, expectLevel, results);
      }
    }
  }

  private static boolean isHiddenFile(FileStatus fileStatus) {
    String name = fileStatus.getPath().getName();
    // the log files is hidden file
    return name.startsWith("_") || (name.startsWith(".") && !name.contains(".log."));
  }

  public static boolean isHiveStylePartitioning(String path) {
    return HIVE_PARTITION_NAME_PATTERN.matcher(path).matches();
  }
}
