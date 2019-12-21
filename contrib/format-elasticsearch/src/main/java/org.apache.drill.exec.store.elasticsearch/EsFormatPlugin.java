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

package org.apache.drill.exec.store.elasticsearch;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.planner.common.DrillStatsTable.TableStatistics;
import org.apache.drill.exec.proto.UserBitShared.CoreOperatorType;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.RecordWriter;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.easy.EasyFormatPlugin;
import org.apache.drill.exec.store.dfs.easy.EasyWriter;
import org.apache.drill.exec.store.dfs.easy.FileWork;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.List;

public class EsFormatPlugin extends EasyFormatPlugin<EsFormatConfig> {

  public static final String DEFAULT_NAME = "elasticsearch";
  private final EsFormatConfig formatConfig;

  public EsFormatPlugin(String name, DrillbitContext context,
                        Configuration fsConf, StoragePluginConfig storageConfig,
                        EsFormatConfig formatConfig) {

    super(name, context, fsConf, storageConfig, formatConfig,
            true,  // readable
            false, // writable
            false, // blockSplittable
            false,  // compressible
            Lists.newArrayList(formatConfig.getExtensions()),
            DEFAULT_NAME);
    this.formatConfig = formatConfig;
  }

  @Override
  public RecordReader getRecordReader(FragmentContext context, DrillFileSystem dfs, FileWork fileWork,
                                      List<SchemaPath> columns, String userName) {
    return new EsRecordReader(context, dfs, fileWork, columns, userName, formatConfig);
  }

  @Override
  public boolean supportsPushDown() {
    // We do not push down filters from sqlline to ES yet.
    //
    return false;
  }

  @Override
  public RecordWriter getRecordWriter(FragmentContext context,
                                      EasyWriter writer) throws UnsupportedOperationException {
    // We do not support writing (posting) to Elasticsearch yet
    //
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public int getReaderOperatorType() {
    return CoreOperatorType.SYSLOG_SUB_SCAN_VALUE;
  }

  @Override
  public int getWriterOperatorType() {
    // We do not support writing (posting) to Elasticsearch yet
    //
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsStatistics() {
    // We do not support statistics yet
    //
    return false;
  }

  @Override
  public TableStatistics readStatistics(FileSystem fs, Path statsTablePath) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void writeStatistics(TableStatistics statistics, FileSystem fs, Path statsTablePath) {
    throw new UnsupportedOperationException("Not implemented");
  }
}
