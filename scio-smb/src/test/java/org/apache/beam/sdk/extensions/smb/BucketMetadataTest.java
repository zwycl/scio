/*
 * Copyright 2019 Spotify AB.
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

import static org.apache.beam.sdk.extensions.smb.SortedBucketIO.DEFAULT_FILENAME_PREFIX;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.Coder.NonDeterministicException;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.smb.BucketMetadata.HashType;
import org.apache.beam.sdk.io.AvroGeneratedUser;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Test;

/** Unit tests for {@link BucketMetadata}. */
public class BucketMetadataTest {

  @Test
  public void testCoding() throws Exception {
    final BucketMetadata<String, String> metadata =
        new TestBucketMetadata(1, 16, 4, HashType.MURMUR3_32, DEFAULT_FILENAME_PREFIX);
    final BucketMetadata<String, String> copy = BucketMetadata.from(metadata.toString());

    Assert.assertEquals(metadata.getVersion(), copy.getVersion());
    Assert.assertEquals(metadata.getNumBuckets(), copy.getNumBuckets());
    Assert.assertEquals(metadata.getNumShards(), copy.getNumShards());
    Assert.assertEquals(metadata.getKeyClass(), copy.getKeyClass());
    Assert.assertEquals(metadata.getHashType(), copy.getHashType());
    Assert.assertEquals(metadata.getFilenamePrefix(), copy.getFilenamePrefix());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testDeterminism() {
    Assert.assertThrows(
        NonDeterministicException.class,
        () ->
            new BucketMetadata(
                BucketMetadata.CURRENT_VERSION,
                1,
                1,
                Double.class,
                HashType.MURMUR3_32,
                DEFAULT_FILENAME_PREFIX) {
              @Override
              public Object extractKey(Object value) {
                return null;
              }

              @Override
              public boolean isPartitionCompatible(BucketMetadata other) {
                return false;
              }
            });
  }

  @Test
  public void testSubTyping() throws Exception {
    final BucketMetadata<String, String> test =
        new TestBucketMetadata(16, 4, HashType.MURMUR3_32, DEFAULT_FILENAME_PREFIX);
    final BucketMetadata<String, GenericRecord> avro =
        new AvroBucketMetadata<>(
            16,
            4,
            String.class,
            HashType.MURMUR3_32,
            "favorite_color",
            DEFAULT_FILENAME_PREFIX,
            AvroGeneratedUser.SCHEMA$);
    final BucketMetadata<String, TableRow> json =
        new JsonBucketMetadata<>(
            16, 4, String.class, HashType.MURMUR3_32, "keyField", DEFAULT_FILENAME_PREFIX);

    Assert.assertEquals(TestBucketMetadata.class, BucketMetadata.from(test.toString()).getClass());
    Assert.assertEquals(AvroBucketMetadata.class, BucketMetadata.from(avro.toString()).getClass());
    Assert.assertEquals(JsonBucketMetadata.class, BucketMetadata.from(json.toString()).getClass());
  }

  @Test
  public void testCompatibility() throws Exception {
    final TestBucketMetadata m1 =
        new TestBucketMetadata(0, 1, 1, HashType.MURMUR3_32, DEFAULT_FILENAME_PREFIX);
    final TestBucketMetadata m2 =
        new TestBucketMetadata(0, 1, 1, HashType.MURMUR3_32, DEFAULT_FILENAME_PREFIX);
    final TestBucketMetadata m3 =
        new TestBucketMetadata(0, 1, 2, HashType.MURMUR3_32, DEFAULT_FILENAME_PREFIX);
    final TestBucketMetadata m4 =
        new TestBucketMetadata(0, 1, 1, HashType.MURMUR3_128, DEFAULT_FILENAME_PREFIX);
    final TestBucketMetadata m5 =
        new TestBucketMetadata(1, 1, 1, HashType.MURMUR3_32, DEFAULT_FILENAME_PREFIX);

    Assert.assertTrue(m1.isCompatibleWith(m2));
    Assert.assertTrue(m1.isCompatibleWith(m3));
    Assert.assertFalse(m1.isCompatibleWith(m4));
    Assert.assertFalse(m1.isCompatibleWith(m5));
  }

  // Test that old BucketMetadata file format missing filenamePrefix will default to "bucket"
  @Test
  public void testFilenamePrefixDefault() throws Exception {
    final String serializedAvro =
        "{\"type\":\"org.apache.beam.sdk.extensions.smb.AvroBucketMetadata\",\"version\":0,\"numBuckets\":2,\"numShards\":1,\"keyClass\":\"java.lang.String\",\"hashType\":\"MURMUR3_32\",\"keyField\":\"user_id\"}";
    Assert.assertEquals(
        DEFAULT_FILENAME_PREFIX,
        ((AvroBucketMetadata) BucketMetadata.from(serializedAvro)).getFilenamePrefix());

    final String serializedJson =
        "{\"type\":\"org.apache.beam.sdk.extensions.smb.JsonBucketMetadata\",\"version\":0,\"numBuckets\":2,\"numShards\":1,\"keyClass\":\"java.lang.String\",\"hashType\":\"MURMUR3_32\",\"keyField\":\"user_id\"}";
    Assert.assertEquals(
        DEFAULT_FILENAME_PREFIX,
        ((JsonBucketMetadata) BucketMetadata.from(serializedJson)).getFilenamePrefix());

    final String serializedTensorflow =
        "{\"type\":\"org.apache.beam.sdk.extensions.smb.TensorFlowBucketMetadata\",\"version\":0,\"numBuckets\":2,\"numShards\":1,\"keyClass\":\"java.lang.String\",\"hashType\":\"MURMUR3_32\",\"keyField\":\"user_id\"}";
    Assert.assertEquals(
        DEFAULT_FILENAME_PREFIX,
        ((TensorFlowBucketMetadata) BucketMetadata.from(serializedTensorflow)).getFilenamePrefix());
  }

  @Test
  public void testNullKeyEncoding() throws Exception {
    final TestBucketMetadata m =
        new TestBucketMetadata(0, 1, 1, HashType.MURMUR3_32, DEFAULT_FILENAME_PREFIX);

    Assert.assertNull(m.extractKey(""));
    Assert.assertNull(m.getKeyBytes(""));
  }

  @Test
  public void testDisplayData() throws Exception {
    final TestBucketMetadata m =
        new TestBucketMetadata(3, 2, 1, HashType.MURMUR3_32, DEFAULT_FILENAME_PREFIX);

    final DisplayData displayData = DisplayData.from(m);
    MatcherAssert.assertThat(displayData, hasDisplayItem("numBuckets", 2));
    MatcherAssert.assertThat(displayData, hasDisplayItem("numShards", 1));
    MatcherAssert.assertThat(displayData, hasDisplayItem("version", 3));
    MatcherAssert.assertThat(displayData, hasDisplayItem("keyClass", String.class));
    MatcherAssert.assertThat(
        displayData, hasDisplayItem("hashType", HashType.MURMUR3_32.toString()));
    MatcherAssert.assertThat(displayData, hasDisplayItem("keyCoder", StringUtf8Coder.class));
  }
}
