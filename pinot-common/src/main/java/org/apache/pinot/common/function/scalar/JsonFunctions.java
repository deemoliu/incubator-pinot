/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.common.function.scalar;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.annotations.VisibleForTesting;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ParseContext;
import com.jayway.jsonpath.Predicate;
import com.jayway.jsonpath.spi.cache.CacheProvider;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import it.unimi.dsi.fastutil.objects.ObjectLinkedOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectSet;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.function.JsonPathCache;
import org.apache.pinot.spi.annotations.ScalarFunction;
import org.apache.pinot.spi.utils.JsonUtils;


/**
 * Inbuilt json related transform functions
 *   An example DimFieldSpec that needs the toJsonMapStr function:
 *   <code>
 *     "dimFieldSpecs": [{
 *       "name": "jsonMapStr",
 *       "dataType": "STRING",
 *       "transformFunction": "toJsonMapStr(jsonMap)"
 *     }]
 *   </code>
 */
public class JsonFunctions {
  private JsonFunctions() {
  }

  private static final Object[] EMPTY = new Object[0];
  private static final Predicate[] NO_PREDICATES = new Predicate[0];
  private static final ParseContext PARSE_CONTEXT = JsonPath.using(
      new Configuration.ConfigurationBuilder().jsonProvider(new ArrayAwareJacksonJsonProvider())
          .mappingProvider(new JacksonMappingProvider()).options(Option.SUPPRESS_EXCEPTIONS)
          .build());

  static {
    // Set the JsonPath cache before the cache is accessed
    CacheProvider.setCache(new JsonPathCache());
  }

  /**
   * Convert Map to Json String
   */
  @ScalarFunction(nullableParameters = true)
  public static String toJsonMapStr(@Nullable Map map)
      throws JsonProcessingException {
    return JsonUtils.objectToString(map);
  }

  @ScalarFunction(nullableParameters = true)
  public static Object[] jsonKeySets(@Nullable Map map)
      throws JsonProcessingException {
    return map.keySet().toArray(new String[0]);
  }

  @ScalarFunction(nullableParameters = true)
  public static String[] jsonKeyValuePrefixes(@Nullable Map map, int maxlength, String splitChar)
      throws JsonProcessingException {
    ObjectSet<String> prefixSet = new ObjectLinkedOpenHashSet<>();
    for (Object key: map.keySet()) {
      Object value = map.get(key);
      String input;
      if (value instanceof String) {
        input = (String) map.get(key);
      } else {
        input = value.toString();
      }
      for (int prefixLength = 1; prefixLength <= maxlength && prefixLength <= input.length(); prefixLength++) {
        if (splitChar != null) {
          prefixSet.add(key + splitChar + input.substring(0, prefixLength));
        } else {
          prefixSet.add(input.substring(0, prefixLength));
        }
      }
    }
    return prefixSet.toArray(new String[0]);
  }

  @ScalarFunction(nullableParameters = true)
  public static String[] jsonKeyValueSuffixes(@Nullable Map map, int maxlength, String splitChar)
      throws JsonProcessingException {
    ObjectSet<String> suffixSet = new ObjectLinkedOpenHashSet<>();
    for (Object key: map.keySet()) {
      Object value = map.get(key);
      String input;
      if (value instanceof String) {
        input = (String) map.get(key);
      } else {
        input = value.toString();
      }
      for (int suffixLength = 1; suffixLength <= maxlength && suffixLength <= input.length(); suffixLength++) {
        if (splitChar != null) {
          suffixSet.add(key + splitChar + input.substring(input.length() - suffixLength));
        } else {
          suffixSet.add(input.substring(input.length() - suffixLength));
        }

      }
    }
    return suffixSet.toArray(new String[0]);
  }

  @ScalarFunction(nullableParameters = true)
  public static String[] jsonKeyValueNgrams(@Nullable Map map, int minGram, int maxGram, String splitChar)
      throws JsonProcessingException {
    ObjectSet<String> ngramSet = new ObjectLinkedOpenHashSet<>();
    for (Object key: map.keySet()) {
      Object value = map.get(key);
      String input;
      if (value instanceof String) {
        input = (String) map.get(key);
      } else {
        input = value.toString();
      }
      for (int n = minGram; n <= maxGram && n <= input.length(); n++) {
        if (n == 0) {
          continue;
        }
        for (int i = 0; i < input.length() - n + 1; i++) {
          if (splitChar != null) {
            ngramSet.add(key + splitChar + input.substring(i, i + n));
          } else {
            ngramSet.add(input.substring(i, i + n));
          }
        }
      }
    }
    return ngramSet.toArray(new String[0]);
  }

  /**
   * Convert object to Json String
   */
  @ScalarFunction(names = {"jsonFormat", "json_format"})
  public static String jsonFormat(Object object)
      throws JsonProcessingException {
    return JsonUtils.objectToString(object);
  }

  /**
   * Extract object based on Json path
   */
  @ScalarFunction(names = {"jsonPath", "json_path"})
  public static Object jsonPath(Object object, String jsonPath) {
    if (object instanceof String) {
      return PARSE_CONTEXT.parse((String) object).read(jsonPath, NO_PREDICATES);
    }
    return PARSE_CONTEXT.parse(object).read(jsonPath, NO_PREDICATES);
  }

  /**
   * Extract object array based on Json path
   */
  @ScalarFunction(names = {"jsonPathArray", "json_path_array"})
  public static Object[] jsonPathArray(Object object, String jsonPath) {
    if (object instanceof String) {
      return convertObjectToArray(PARSE_CONTEXT.parse((String) object).read(jsonPath, NO_PREDICATES));
    }
    return convertObjectToArray(PARSE_CONTEXT.parse(object).read(jsonPath, NO_PREDICATES));
  }

  @ScalarFunction(nullableParameters = true, names = {"jsonPathArrayDefaultEmpty", "json_path_array_default_empty"})
  public static Object[] jsonPathArrayDefaultEmpty(@Nullable Object object, String jsonPath) {
    try {
      Object[] result = object == null ? null : jsonPathArray(object, jsonPath);
      return result == null ? EMPTY : result;
    } catch (Exception e) {
      return EMPTY;
    }
  }

  private static Object[] convertObjectToArray(Object arrayObject) {
    if (arrayObject instanceof List) {
      return ((List) arrayObject).toArray();
    } else if (arrayObject instanceof Object[]) {
      return (Object[]) arrayObject;
    } else if (arrayObject == null) {
      return null;
    }
    return new Object[]{arrayObject};
  }

  /**
   * Extract from Json with path to String
   */
  @Nullable
  @ScalarFunction(names = {"jsonPathString", "json_path_string"})
  public static String jsonPathString(Object object, String jsonPath)
      throws JsonProcessingException {
    Object jsonValue = jsonPath(object, jsonPath);
    if (jsonValue instanceof String) {
      return (String) jsonValue;
    }
    return jsonValue == null ? null : JsonUtils.objectToString(jsonValue);
  }

  /**
   * Extract from Json with path to String
   */
  @ScalarFunction(nullableParameters = true, names = {"jsonPathString", "json_path_string"})
  public static String jsonPathString(@Nullable Object object, String jsonPath, String defaultValue) {
    try {
      Object jsonValue = jsonPath(object, jsonPath);
      if (jsonValue instanceof String) {
        return (String) jsonValue;
      }
      return jsonValue == null ? defaultValue : JsonUtils.objectToString(jsonValue);
    } catch (Exception ignore) {
      return defaultValue;
    }
  }

  /**
   * Extract from Json with path to Long
   */
  @ScalarFunction(names = {"jsonPathLong", "json_path_long"})
  public static long jsonPathLong(Object object, String jsonPath) {
    return jsonPathLong(object, jsonPath, Long.MIN_VALUE);
  }

  /**
   * Extract from Json with path to Long
   */
  @ScalarFunction(nullableParameters = true, names = {"jsonPathLong", "json_path_long"})
  public static long jsonPathLong(@Nullable Object object, String jsonPath, long defaultValue) {
    try {
      Object jsonValue = jsonPath(object, jsonPath);
      if (jsonValue == null) {
        return defaultValue;
      }
      if (jsonValue instanceof Number) {
        return ((Number) jsonValue).longValue();
      }
      return Long.parseLong(jsonValue.toString());
    } catch (Exception ignore) {
      return defaultValue;
    }
  }

  /**
   * Extract from Json with path to Double
   */
  @ScalarFunction(names = {"jsonPathDouble", "json_path_double"})
  public static double jsonPathDouble(Object object, String jsonPath) {
    return jsonPathDouble(object, jsonPath, Double.NaN);
  }

  /**
   * Extract from Json with path to Double
   */
  @ScalarFunction(nullableParameters = true, names = {"jsonPathDouble", "json_path_double"})
  public static double jsonPathDouble(@Nullable Object object, String jsonPath, double defaultValue) {
    try {
      Object jsonValue = jsonPath(object, jsonPath);
      if (jsonValue == null) {
        return defaultValue;
      }
      if (jsonValue instanceof Number) {
        return ((Number) jsonValue).doubleValue();
      }
      return Double.parseDouble(jsonValue.toString());
    } catch (Exception ignore) {
      return defaultValue;
    }
  }

  @VisibleForTesting
  static class ArrayAwareJacksonJsonProvider extends JacksonJsonProvider {
    @Override
    public boolean isArray(Object obj) {
      return (obj instanceof List) || (obj instanceof Object[]);
    }

    @Override
    public Object getArrayIndex(Object obj, int idx) {
      if (obj instanceof Object[]) {
        return ((Object[]) obj)[idx];
      }
      return super.getArrayIndex(obj, idx);
    }

    @Override
    public int length(Object obj) {
      if (obj instanceof Object[]) {
        return ((Object[]) obj).length;
      }
      return super.length(obj);
    }

    @Override
    public Iterable<?> toIterable(Object obj) {
      if (obj instanceof Object[]) {
        return Arrays.asList((Object[]) obj);
      }
      return super.toIterable(obj);
    }
  }
}
