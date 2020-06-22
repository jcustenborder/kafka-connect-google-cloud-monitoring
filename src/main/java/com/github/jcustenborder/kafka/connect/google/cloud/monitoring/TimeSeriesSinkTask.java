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

import com.github.jcustenborder.kafka.connect.utils.VersionUtil;
import com.google.api.Metric;
import com.google.api.MonitoredResource;
import com.google.monitoring.v3.CreateTimeSeriesRequest;
import com.google.monitoring.v3.Point;
import com.google.monitoring.v3.ProjectName;
import com.google.monitoring.v3.TimeInterval;
import com.google.monitoring.v3.TimeSeries;
import com.google.monitoring.v3.TypedValue;
import com.google.protobuf.util.Timestamps;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TimeSeriesSinkTask extends SinkTask {
  private static final Logger log = LoggerFactory.getLogger(TimeSeriesSinkTask.class);

  @Override
  public String version() {
    return VersionUtil.version(this.getClass());
  }

  MetricServiceFactory metricServiceFactory = new MetricServiceFactory() {
  };

  MetricService metricService;

  TimeSeriesSinkConnectorConfig config;
  ProjectName projectName;


  @Override
  public void start(Map<String, String> settings) {
    this.config = new TimeSeriesSinkConnectorConfig(settings);
    this.projectName = ProjectName.of(Long.toString(this.config.projectID));

    try {
      this.metricService = this.metricServiceFactory.create(this.config);
    } catch (IOException e) {
      ConfigException configException = new ConfigException("Exception thrown while creating MetricServiceClient");
      configException.initCause(e);
      throw configException;
    }
  }


  @Override
  public void put(Collection<SinkRecord> records) {
    CreateTimeSeriesRequest.Builder createTimeSeriesRequestBuilder = CreateTimeSeriesRequest
        .newBuilder()
        .setName(this.projectName.toString());

    for (SinkRecord record : records) {
      if (null == record.value()) {
        log.trace("put() - Skipping record because it's a delete");
        continue;
      }
      if (!(record.value() instanceof Struct) && !(record.value() instanceof Map)) {
        throw new DataException(
            "record.value() must be a Struct or Map"
        );
      }

      TimeInterval timeInterval = TimeInterval.newBuilder()
          .setEndTime(Timestamps.fromMillis(record.timestamp()))
          .build();

      Map<String, String> resourceLabels = addLabels(record.value());
      log.trace("put() - resourceLabels = '{}'", resourceLabels);
      MonitoredResource monitoredResource = MonitoredResource.newBuilder()
          .setType(this.config.resourceType)
          .putAllLabels(resourceLabels)
          .build();

      if (TimeSeriesSinkConnectorConfig.MetricNameType.NameField == this.config.metricNameType) {
        String nameValue = getFieldValue(this.config.metricNameField, record.value(), String.class);
        Path path = Paths.get(this.config.metricTypePrefix, nameValue);
        Number value = getFieldValue(this.config.metricValueField, record.value(), Number.class);
        log.trace("put() - metric path = '{}' value = '{}'", path, value);
        TypedValue typedValue = TypedValue.newBuilder()
            .setDoubleValue(value.doubleValue())
            .build();
        Metric metric = Metric.newBuilder()
            .setType(path.toString())
            .build();
        List<Point> points = Arrays.asList(
            Point.newBuilder()
                .setInterval(timeInterval)
                .setValue(typedValue)
                .build()
        );

        TimeSeries timeSeries =
            TimeSeries.newBuilder()
                .setMetric(metric)
                .setResource(monitoredResource)
                .addAllPoints(points)
                .build();
        createTimeSeriesRequestBuilder.addTimeSeries(timeSeries);
      } else if (TimeSeriesSinkConnectorConfig.MetricNameType.MetricFields == this.config.metricNameType) {
        for (String metricFieldName : this.config.metricFields) {
          Path path = Paths.get(this.config.metricTypePrefix, metricFieldName);
          Number value = getFieldValue(metricFieldName, record.value(), Number.class);
          log.trace("put() - metric path = '{}' value = '{}'", path, value);
          if (null != value) {
            TypedValue typedValue = TypedValue.newBuilder()
                .setDoubleValue(value.doubleValue())
                .build();
            Metric metric = Metric.newBuilder()
                .setType(path.toString())
                .build();
            List<Point> points = Arrays.asList(
                Point.newBuilder()
                    .setInterval(timeInterval)
                    .setValue(typedValue)
                    .build()
            );

            TimeSeries timeSeries =
                TimeSeries.newBuilder()
                    .setMetric(metric)
                    .setResource(monitoredResource)
                    .addAllPoints(points)
                    .build();
            createTimeSeriesRequestBuilder.addTimeSeries(timeSeries);
          }
        }
      }

      CreateTimeSeriesRequest createTimeSeriesRequest = createTimeSeriesRequestBuilder.build();
      log.trace("put() - Calling createTimeSeries\n{}", createTimeSeriesRequest);
      metricService.createTimeSeries(createTimeSeriesRequest);
    }
  }

  private Map<String, String> addLabels(Object value) {
    Map<String, String> result = new LinkedHashMap<>();
    for (String fieldName : this.config.resourceLabelFields) {
      Object fieldValue;
      if (value instanceof Struct) {
        Struct struct = (Struct) value;
        fieldValue = struct.get(fieldName);
      } else if (value instanceof Map) {
        Map map = (Map) value;
        fieldValue = map.get(fieldName);
      } else {
        fieldValue = null;
      }

      if (fieldValue instanceof String) {
        result.put(fieldName, fieldValue.toString());
      } else if (fieldValue instanceof Map) {
        Map<String, Object> map = (Map<String, Object>) fieldValue;
        map.forEach((k, v) -> result.put(k, v.toString()));
      }
    }
    return result;
  }

  private <T> T getFieldValue(String fieldName, Object value, Class<T> cls) {
    Object fieldValue;
    if (value instanceof Struct) {
      Struct struct = (Struct) value;
      fieldValue = struct.get(fieldName);
    } else if (value instanceof Map) {
      Map map = (Map) value;
      fieldValue = map.get(fieldName);
    } else {
      fieldValue = null;
    }
    try {
      return cls.cast(fieldValue);
    } catch (ClassCastException ex) {
      throw new IllegalStateException(ex);
    }
  }

  @Override
  public void stop() {
    metricService.close();
  }
}
