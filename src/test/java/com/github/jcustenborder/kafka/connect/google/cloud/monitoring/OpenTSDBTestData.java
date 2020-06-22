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

import com.google.common.base.Preconditions;

import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class OpenTSDBTestData {

  static final Pattern PREFIX = Pattern.compile("^(\\S+) (\\d+) ([\\d.]+)(.+)$");
  static final Pattern KVP = Pattern.compile("\\s+(\\S+)=(\\S+)");

  static final List<String> DATA = Arrays.asList(
      "mysql.bytes_received 1287333217 327810227706 schema=foo host=db1",
      "mysql.bytes_sent 1287333217 6604859181710 schema=foo host=db1",
      "mysql.bytes_received 1287333232 327812421706 schema=foo host=db1",
      "mysql.bytes_sent 1287333232 6604901075387 schema=foo host=db1",
      "mysql.bytes_received 1287333321 340899533915 schema=foo host=db2",
      "mysql.bytes_sent 1287333321 5506469130707 schema=foo host=db2"
  );

  public static class Value {
    public final String metric;
    public final Date timestamp;
    public final Double value;
    public final Map<String, String> tags;

    public Value(String metric, Date timestamp, Double value, Map<String, String> tags) {
      this.metric = metric;
      this.timestamp = timestamp;
      this.value = value;
      this.tags = tags;
    }

    public static Value parse(String text) {
      Matcher matcher = PREFIX.matcher(text);
      Preconditions.checkArgument(matcher.matches(), "text does not match regex('%s')", PREFIX.pattern());
      String metric = matcher.group(1);
      String timestampString = matcher.group(2);
      Long t = Long.parseLong(timestampString);
      if (timestampString.length() <= 10) {
        t = t * 1000L;
      }
      Date timestamp = new Date(t);
      String valueString = matcher.group(3);
      double value = Double.parseDouble(valueString);
      String tagText = matcher.group(4);
      Map<String, String> tags = new LinkedHashMap<>();
      matcher = KVP.matcher(tagText);
      while (matcher.find()) {
        String k = matcher.group(1);
        String v = matcher.group(2);
        tags.put(k, v);
      }
      return new Value(metric, timestamp, value, tags);
    }
  }

  public static List<Value> getData() {
    return DATA.stream()
        .map(Value::parse)
        .collect(Collectors.toList());
  }


}
