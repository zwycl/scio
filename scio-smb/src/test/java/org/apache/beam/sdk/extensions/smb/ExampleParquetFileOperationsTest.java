/*
 * Copyright 2020 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.beam.sdk.extensions.smb;

import com.google.protobuf.ByteString;
import me.lyh.parquet.tensorflow.Schema;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tensorflow.example.*;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.beam.sdk.extensions.smb.TestUtils.fromFolder;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;

/** Unit tests for {@link ExampleParquetFileOperations}. */
public class ExampleParquetFileOperationsTest {
  @Rule public final TemporaryFolder output = new TemporaryFolder();

  private final Schema schema = Schema.newBuilder()
      .required("int64_req", Schema.Type.INT64)
      .required("float_req", Schema.Type.FLOAT)
      .required("bytes_req", Schema.Type.BYTES)
      .optional("int64_opt", Schema.Type.INT64)
      .optional("float_opt", Schema.Type.FLOAT)
      .optional("bytes_opt", Schema.Type.BYTES)
      .repeated("int64_rep", Schema.Type.INT64)
      .repeated("float_rep", Schema.Type.FLOAT)
      .repeated("bytes_rep", Schema.Type.BYTES)
      .named("Example");

  private final List<Example> examples = IntStream
      .range(0, 10)
      .mapToObj(this::newExample)
      .collect(Collectors.toList());

  @Test
  public void testUncompressed() throws Exception {
    test(null, CompressionCodecName.UNCOMPRESSED);
  }

  @Test
  public void testSnappy() throws Exception {
    test(null, CompressionCodecName.SNAPPY);
  }

  @Test
  public void testProjection() throws Exception {
    List<String> fields = Stream.of("int64_req", "int64_opt", "int64_rep").collect(Collectors.toList());
    test(fields, CompressionCodecName.SNAPPY);
  }

  private void test(List<String> fields, CompressionCodecName codec) throws Exception {
    final ExampleParquetFileOperations fileOperations = ExampleParquetFileOperations.of(schema, fields, codec);
    final ResourceId file = fromFolder(output).resolve("file.parquet", ResolveOptions.StandardResolveOptions.RESOLVE_FILE);

    FileOperations.Writer<Example> writer = fileOperations.createWriter(file);
    for (Example example : examples) {
      writer.write(example);
    }
    writer.close();

    final List<Example> actual = new ArrayList<>();
    fileOperations.iterator(file).forEachRemaining(actual::add);

    List<Example> expected;
    if (fields == null) {
      expected = examples;
    } else {
      Function<Example, Example> fn = getFeatures(new HashSet<>(fields));
      expected = examples.stream().map(fn).collect(Collectors.toList());
    }
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testDisplayData() {
    final ExampleParquetFileOperations fileOperations = ExampleParquetFileOperations.of(
        schema, Collections.singletonList("int64_req"), CompressionCodecName.SNAPPY);

    final DisplayData displayData = DisplayData.from(fileOperations);
    MatcherAssert.assertThat(
        displayData, hasDisplayItem("FileOperations", ExampleParquetFileOperations.class));
    MatcherAssert.assertThat(displayData, hasDisplayItem("mimeType", MimeTypes.BINARY));
    MatcherAssert.assertThat(
        displayData, hasDisplayItem("compression", Compression.UNCOMPRESSED.toString()));
    MatcherAssert.assertThat(displayData, hasDisplayItem("writerSchema", schema.toJson()));
    MatcherAssert.assertThat(displayData, hasDisplayItem("readerFields", "[int64_req]"));
    MatcherAssert.assertThat(
        displayData, hasDisplayItem("codec", CompressionCodecName.SNAPPY.name()));
  }

  private Feature longs(long... xs) {
    return Feature
        .newBuilder()
        .setInt64List(Int64List
            .newBuilder()
            .addAllValue(Arrays.stream(xs).boxed().collect(Collectors.toList())))
        .build();
  }

  private Feature floats(float... xs) {
    FloatList.Builder builder = FloatList.newBuilder();
    for (float x : xs) {
      builder = builder.addValue(x);
    }
    return Feature.newBuilder().setFloatList(builder).build();
  }

  private Feature bytes(String... xs) {
    return Feature
        .newBuilder()
        .setBytesList(BytesList
            .newBuilder()
            .addAllValue(Arrays
                .stream(xs)
                .map(ByteString::copyFromUtf8)
                .collect(Collectors.toList())))
        .build();
  }

  private Example newExample(int i) {
    Features.Builder builder = Features.newBuilder()
        .putFeature("int64_req", longs(i))
        .putFeature("float_req", floats(i))
        .putFeature("bytes_req", bytes("bytes" + i))
        .putFeature("int64_rep", longs(i, i, i))
        .putFeature("float_rep", floats(i, i, i))
        .putFeature("bytes_rep", bytes("a" + i, "b" + i, "c" + i));
    if (i % 2 == 0) {
      builder = builder
          .putFeature("int64_opt", longs(i))
          .putFeature("float_opt", floats(i))
          .putFeature("bytes_opt", bytes("bytes" + i));
    }
    return Example.newBuilder().setFeatures(builder).build();
  }

  private Function<Example, Example> getFeatures(Set<String> fields) {
    return new Function<Example, Example>() {
      @Override
      public Example apply(Example example) {
        Features.Builder builder = Features.newBuilder();
        for (Map.Entry<String, Feature> kv : example.getFeatures().getFeatureMap().entrySet()) {
          if (fields.contains(kv.getKey())) {
            builder = builder.putFeature(kv.getKey(), kv.getValue());
          }
        }
        return Example.newBuilder().setFeatures(builder).build();
      }
    };
  }
}
