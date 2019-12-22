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

import io.netty.buffer.DrillBuf;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.easy.FileWork;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.realityforge.jsyslog.message.StructuredDataParameter;
import org.realityforge.jsyslog.message.SyslogMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;

public class EsRecordReader extends AbstractRecordReader {

  private static final Logger logger = LoggerFactory.getLogger(EsRecordReader.class);
  private static final int MAX_RECORDS_PER_BATCH = 4096;

  private final DrillFileSystem fileSystem;
  private final FileWork fileWork;
  private final String userName;
  private BufferedReader reader;
  private RestHighLevelClient esClient;
  private DrillBuf buffer;
  private VectorContainerWriter writer;
  private final int maxErrors;
  private final boolean flattenStructuredData;
  private int errorCount;
  private int lineCount;
  private final List<SchemaPath> projectedColumns;
  private String line;
  private boolean isOpen = false;

  public EsRecordReader(FragmentContext context,
                            DrillFileSystem fileSystem,
                            FileWork fileWork,
                            List<SchemaPath> columns,
                            String userName,
                            EsFormatConfig config) throws OutOfMemoryException {

    this.fileSystem = fileSystem;
    this.fileWork = fileWork;
    this.userName = userName;
    this.maxErrors = config.getMaxErrors();
    this.errorCount = 0;
    this.buffer = context.getManagedBuffer().reallocIfNeeded(4096);
    this.projectedColumns = columns;
    this.flattenStructuredData = config.getFlattenStructuredData();

    setColumns(columns);
  }

  @Override
  public void setup(final OperatorContext context, final OutputMutator output) {
    openFile();
    this.writer = new VectorContainerWriter(output);
  }

  private void openFile() {
    InputStream in;
    try {
      in = fileSystem.openPossiblyCompressedStream(fileWork.getPath());
    } catch (Exception e) {
      throw UserException
              .dataReadError(e)
              .message("Failed to open input file: %s", fileWork.getPath())
              .addContext("User name", this.userName)
              .build(logger);
    }
    this.lineCount = 0;
    reader = new BufferedReader(new InputStreamReader(in));

    try {
      // A single host:port:user:password
      //
      String[] connInfo = reader.readLine().split(":");

      // Currently we only support connecting to a single ES instance.
      // Error out if format of a single (host:port:user:password) is not followed.
      //
      if (connInfo.length != 4) throw new IOException("Required format is host:port");

      isOpen = true;

      final CredentialsProvider credentialsProvider =
              new BasicCredentialsProvider();
      credentialsProvider.setCredentials(AuthScope.ANY,
              new UsernamePasswordCredentials(connInfo[2], connInfo[3]));

      RestClientBuilder restClientBuilder = RestClient.builder(
              new HttpHost(connInfo[0], Integer.parseInt(connInfo[1])))
              .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder
                      .setDefaultCredentialsProvider(credentialsProvider));

      esClient = new RestHighLevelClient(restClientBuilder);

      // Create the ES search query
      //
      SearchRequest searchRequest = new SearchRequest();
      SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
      searchSourceBuilder.query(QueryBuilders.matchAllQuery());
      searchRequest.source(searchSourceBuilder);

      // Run the query
      //
      line = esClient.search(searchRequest).toString();
      // Trim the result
      //
      line = line.trim();

      lineCount++;
    } catch (Exception e)
    {
      throw UserException
              .dataReadError(e)
              .message("Failed to parse input: %s", fileWork.getPath())
              .addContext("User name", this.userName)
              .build(logger);
    }
  }

  @Override
  public int next() {
    this.writer.allocate();
    this.writer.reset();

    int recordCount = 0;

    try {
      BaseWriter.MapWriter map = this.writer.rootAsMap();

      try {
        this.writer.setPosition(recordCount);

        // Map the string response to varchar in drill
        //
        map.start();

        byte[] bytes = line.getBytes(StandardCharsets.UTF_8);
        buffer = this.buffer.reallocIfNeeded(bytes.length);
        buffer.setBytes(0, bytes, 0, bytes.length);
        map.varChar("Elasticsearch query result").writeVarChar(0, bytes.length, buffer);

        map.end();

        if(isOpen) {
          // Update record number so writer index is correct.
          //
          recordCount++;
          isOpen = false;
        }

      } catch (Exception e) {
        errorCount++;
        if (errorCount > maxErrors) {
          throw UserException
                  .dataReadError()
                  .message("Maximum Error Threshold Exceeded: ")
                  .addContext("Line: " + lineCount)
                  .addContext(e.getMessage())
                  .build(logger);
        }
      }

      this.writer.setValueCount(recordCount);
      return recordCount;

    } catch (final Exception e) {
      errorCount++;
      if (errorCount > maxErrors) {
        throw UserException.dataReadError()
                .message("Error parsing file")
                .addContext(e.getMessage())
                .build(logger);
      }
    }

    return recordCount;
  }

  // Helper function to map strings
  //
  private void mapStringField(String name, String value, BaseWriter.MapWriter map) {
    if (value == null) {
      return;
    }
    try {
      byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
      int stringLength = bytes.length;
      this.buffer = buffer.reallocIfNeeded(stringLength);
      this.buffer.setBytes(0, bytes, 0, stringLength);
      map.varChar(name).writeVarChar(0, stringLength, buffer);
    } catch (Exception e) {
      throw UserException
              .dataWriteError()
              .addContext("Could not write string: ")
              .addContext(e.getMessage())
              .build(logger);
    }
  }

  // Helper function to flatten structured data
  //
  private void mapFlattenedStructuredData(Map<String, List<StructuredDataParameter>> data, BaseWriter.MapWriter map) {
    for (Map.Entry<String, List<StructuredDataParameter>> entry : data.entrySet()) {
      for (StructuredDataParameter parameter : entry.getValue()) {
        String fieldName = "structured_data_" + parameter.getName();
        String fieldValue = parameter.getValue();
        mapStringField(fieldName, fieldValue, map);
      }
    }
  }

  // Gets field from the Structured Data Construct
  //
  private String getFieldFromStructuredData(String fieldName, SyslogMessage parsedMessage) {
    for (Map.Entry<String, List<StructuredDataParameter>> entry : parsedMessage.getStructuredData().entrySet()) {
      for (StructuredDataParameter d : entry.getValue()) {
        if (d.getName().equals(fieldName)) {
          return d.getValue();
        }
      }
    }
    return null;
  }

  // Helper function to map arrays
  //
  private void mapComplexField(String mapName, Map<String, List<StructuredDataParameter>> data, BaseWriter.MapWriter map) {
    for (Map.Entry<String, List<StructuredDataParameter>> entry : data.entrySet()) {
      List<StructuredDataParameter> dataParameters = entry.getValue();
      String fieldName;
      String fieldValue;

      for (StructuredDataParameter parameter : dataParameters) {
        fieldName = parameter.getName();
        fieldValue = parameter.getValue();

        VarCharHolder rowHolder = new VarCharHolder();

        byte[] rowStringBytes = fieldValue.getBytes();
        this.buffer.reallocIfNeeded(rowStringBytes.length);
        this.buffer.setBytes(0, rowStringBytes);
        rowHolder.start = 0;
        rowHolder.end = rowStringBytes.length;
        rowHolder.buffer = this.buffer;

        map.map(mapName).varChar(fieldName).write(rowHolder);
      }
    }
  }

  // Helper function to map simple dates
  //
  public SimpleDateFormat getValidDateObject(String d) {
    SimpleDateFormat tempDateFormat;
    if (d != null && !d.isEmpty()) {
      tempDateFormat = new SimpleDateFormat(d);
    } else {
      throw UserException
              .parseError()
              .message("Invalid date format")
              .build(logger);
    }
    return tempDateFormat;
  }

  public void close() throws Exception {
    this.esClient.close();
    this.reader.close();
  }
}
