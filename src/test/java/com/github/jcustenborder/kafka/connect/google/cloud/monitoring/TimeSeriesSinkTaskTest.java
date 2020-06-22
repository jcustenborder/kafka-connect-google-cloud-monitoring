/**
 * Copyright © 2019 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.google.cloud.monitoring;

import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.mockito.Mockito.mock;

public class TimeSeriesSinkTaskTest {
  private static final Logger log = LoggerFactory.getLogger(TimeSeriesSinkTaskTest.class);
  TimeSeriesSinkTask task;

  @BeforeEach
  public void setup() {
    this.task = new TimeSeriesSinkTask();
    this.task.metricService = mock(MetricService.class);
  }

  @Disabled
  @Test
  public void putMultipleFields() {
    Map<String, String> settings = new LinkedHashMap<>();
    settings.put(TimeSeriesSinkConnectorConfig.PROJECT_ID_CONFIG, "1234123");
    settings.put(TimeSeriesSinkConnectorConfig.METRIC_NAME_PREFIX_CONF, "example.com/mysql");
    settings.put(TimeSeriesSinkConnectorConfig.METRIC_FIELDS_CONF, "bytes_received,bytes_sent");
    settings.put(TimeSeriesSinkConnectorConfig.RESOURCE_TYPE_CONF, "generic_node");
    settings.put(TimeSeriesSinkConnectorConfig.METRIC_NAME_TYPE_CONF, TimeSeriesSinkConnectorConfig.MetricNameType.MetricFields.toString());
    settings.put(TimeSeriesSinkConnectorConfig.RESOURCE_LABEL_FIELDS_CONF, "host,schema");

    this.task.start(settings);
    List<SinkRecord> records = new ArrayList<>();
    Schema schema = SchemaBuilder.struct()
        .name("metric")
        .field("bytes_received", Schema.FLOAT64_SCHEMA)
        .field("bytes_sent", Schema.FLOAT64_SCHEMA)
        .field("schema", Schema.STRING_SCHEMA)
        .field("host", Schema.STRING_SCHEMA)
        .field("timestamp", Timestamp.SCHEMA)
        .build();

    long offset = 123412L;

    Multimap<Map<String, String>, OpenTSDBTestData.Value> multimap = LinkedHashMultimap.create();
    OpenTSDBTestData.getData().forEach(v -> multimap.put(v.tags, v));


    Pattern metricNamePattern = Pattern.compile("^mysql\\.(.+)$");
    for (Map<String, String> tags : multimap.keySet()) {

      Collection<OpenTSDBTestData.Value> values = multimap.get(tags);
      Struct struct = new Struct(schema);

      tags.forEach(struct::put);
      Date timestamp = null;
      for (OpenTSDBTestData.Value value : values) {
        timestamp = value.timestamp;
        struct.put("timestamp", value.timestamp);
        Matcher metricMatcher = metricNamePattern.matcher(value.metric);
        if (metricMatcher.find()) {
          struct.put(metricMatcher.group(1), value.value);
        }
      }
      log.info("{}", struct);
      SinkRecord record = new SinkRecord(
          "test",
          0,
          null,
          null,
          schema,
          struct,
          offset++,
          (null != timestamp ? timestamp.getTime() : 1234123L),
          TimestampType.LOG_APPEND_TIME
      );
      records.add(record);
    }

    this.task.put(records);
  }


  @Disabled
  @Test
  public void putSingleField() throws IOException {
    Map<String, String> settings = new LinkedHashMap<>();
    settings.put(TimeSeriesSinkConnectorConfig.PROJECT_ID_CONFIG, "1234123");
    settings.put(TimeSeriesSinkConnectorConfig.RESOURCE_TYPE_CONF, "generic_node");
    settings.put(TimeSeriesSinkConnectorConfig.METRIC_NAME_PREFIX_CONF, "custom.googleapis.com");
    settings.put(TimeSeriesSinkConnectorConfig.METRIC_NAME_FIELD_CONF, "metric");
    settings.put(TimeSeriesSinkConnectorConfig.METRIC_VALUE_FIELD_CONF, "value");
    settings.put(TimeSeriesSinkConnectorConfig.METRIC_NAME_TYPE_CONF, TimeSeriesSinkConnectorConfig.MetricNameType.NameField.toString());
    settings.put(TimeSeriesSinkConnectorConfig.RESOURCE_LABEL_FIELDS_CONF, "tags");

    this.task.start(settings);
    List<SinkRecord> records = new ArrayList<>();
    Schema schema = SchemaBuilder.struct()
        .name("metric")
        .field("metric", Schema.STRING_SCHEMA)
        .field("value", Schema.FLOAT64_SCHEMA)
        .field("tags", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA))
        .field("timestamp", Timestamp.SCHEMA)
        .build();

    long offset = 123412L;
    for (OpenTSDBTestData.Value value : OpenTSDBTestData.getData()) {
      Struct struct = new Struct(schema)
          .put("metric", value.metric.replace('.', '/'))
          .put("value", value.value)
          .put("tags", value.tags)
          .put("timestamp", value.timestamp);
      SinkRecord record = new SinkRecord(
          "test",
          0,
          null,
          null,
          schema,
          struct,
          offset++,
          value.timestamp.getTime(),
          TimestampType.LOG_APPEND_TIME
      );
      records.add(record);
    }

    this.task.put(records);
  }


}
