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

import me.lyh.parquet.tensorflow.ExampleParquetReader;
import me.lyh.parquet.tensorflow.ExampleParquetWriter;
import me.lyh.parquet.tensorflow.Schema;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.parquet.Preconditions;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.*;
import org.tensorflow.example.Example;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * {@link org.apache.beam.sdk.extensions.smb.FileOperations} implementation for TensorFlow
 * {@link org.tensorflow.example.Example} as Parquet files.
 */
public class ExampleFileOperations extends FileOperations<Example> {
  private static final CompressionCodecName DEFAULT_CODEC = CompressionCodecName.SNAPPY;

  private final String writerSchemaString;
  private final List<String> readerFields;
  private final CompressionCodecName codec;

  private ExampleFileOperations(String writerSchemaString,
                                List<String> readerFields,
                                CompressionCodecName codec) {
    super(Compression.UNCOMPRESSED, MimeTypes.BINARY);
    this.writerSchemaString = writerSchemaString;
    this.readerFields = readerFields;
    this.codec = codec;
  }

  // read all fields
  public static ExampleFileOperations of(Schema writerSchema) {
    return of(writerSchema, null, DEFAULT_CODEC);
  }

  // read all fields
  public static ExampleFileOperations of(Schema writerSchema, CompressionCodecName codec) {
    return new ExampleFileOperations(writerSchema.toJson(), null, codec);
  }

  // read with projection
  public static ExampleFileOperations of(Schema writerSchema,
                                         List<String> readerFields) {
    return of(writerSchema, readerFields, DEFAULT_CODEC);
  }

  // read with projection
  public static ExampleFileOperations of(Schema writerSchema,
                                         List<String> readerFields,
                                         CompressionCodecName codec) {
    return new ExampleFileOperations(writerSchema.toJson(), readerFields, codec);
  }

  // read all fields
  public static ExampleFileOperations readOnly() {
    return readOnly(null, DEFAULT_CODEC);
  }

  // read with projection
  public static ExampleFileOperations readOnly(List<String> readerFields) {
    return readOnly(readerFields, DEFAULT_CODEC);
  }

  // read with projection
  public static ExampleFileOperations readOnly(List<String> readerFields,
                                               CompressionCodecName codec) {
    return of(null, readerFields, codec);
  }

  @Override
  protected Reader<Example> createReader() {
    return new ExampleReader(readerFields);
  }

  @Override
  protected FileIO.Sink<Example> createSink() {
    return new ExampleSink();
  }

  private class ExampleSink implements FileIO.Sink<Example>, Closeable {

    private transient ParquetWriter<Example> writer;

    @Override
    public void open(WritableByteChannel channel) throws IOException {
      writer = ExampleParquetWriter.builder(new BeamOutputFile(channel))
          .withCompressionCodec(codec)
          .withSchema(Schema.fromJson(writerSchemaString))
          .build();
    }

    @Override
    public void write(Example element) throws IOException {
      writer.write(element);
    }

    @Override
    public void flush() throws IOException { }

    @Override
    public void close() throws IOException {
      writer.close();
    }
  }

  @Override
  public Coder<Example> getCoder() {
    return ProtoCoder.of(Example.class);
  }

  ////////////////////////////////////////
  // Reader
  ////////////////////////////////////////

  private static class ExampleReader extends FileOperations.Reader<Example> {
    private List<String> fields;
    private transient ParquetReader<Example> reader;
    private transient Example next;

    ExampleReader(List<String> fields) {
      this.fields = fields;
    }

    @Override
    public void prepareRead(ReadableByteChannel channel) throws IOException {
      ExampleParquetReader.Builder builder =
          ExampleParquetReader.builder(new BeamInputFile((SeekableByteChannel) channel));

      builder = fields == null ? builder : builder.withFields(fields);

      reader = builder.build();
    }

    @Override
    public Example readNext() throws IOException, NoSuchElementException {
      next = reader.read();
      if (next == null) {
        throw new NoSuchElementException();
      }
      return next;
    }

    @Override
    public boolean hasNextElement() throws IOException {
      next = reader.read();
      return next != null;
    }

    @Override
    public void finishRead() throws IOException {
      reader.close();
    }
  }

  ////////////////////////////////////////
  // Parquet {Input,Output}File
  ////////////////////////////////////////

  private static class BeamInputFile implements InputFile {

    private final SeekableByteChannel channel;

    private BeamInputFile(SeekableByteChannel channel) {
      this.channel = channel;
    }

    @Override
    public long getLength() throws IOException {
      return channel.size();
    }

    @Override
    public SeekableInputStream newStream() throws IOException {
      return new BeamInputStream(channel);
    }

    private static class BeamInputStream extends DelegatingSeekableInputStream {
      private final SeekableByteChannel channel;

      private BeamInputStream(SeekableByteChannel channel) {
        super(Channels.newInputStream(channel));
        this.channel = channel;
      }

      @Override
      public long getPos() throws IOException {
        return channel.position();
      }

      @Override
      public void seek(long newPos) throws IOException {
        channel.position(newPos);
      }
    }
  }

  private static class BeamOutputFile implements OutputFile {

    private OutputStream outputStream;

    private BeamOutputFile(WritableByteChannel channel) {
      this.outputStream = Channels.newOutputStream(channel);
    }

    @Override
    public PositionOutputStream create(long blockSizeHint) {
      return new BeamOutputStream(outputStream);
    }

    @Override
    public PositionOutputStream createOrOverwrite(long blockSizeHint) {
      return new BeamOutputStream(outputStream);
    }

    @Override
    public boolean supportsBlockSize() {
      return false;
    }

    @Override
    public long defaultBlockSize() {
      return 0;
    }

    private class BeamOutputStream extends PositionOutputStream {
      private long position = 0;
      private OutputStream outputStream;

      private BeamOutputStream(OutputStream outputStream) {
        this.outputStream = outputStream;
      }

      @Override
      public long getPos() throws IOException {
        return position;
      }

      @Override
      public void write(int b) throws IOException {
        position++;
        outputStream.write(b);
      }

      @Override
      public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
      }

      @Override
      public void write(byte[] b, int off, int len) throws IOException {
        outputStream.write(b, off, len);
        position += len;
      }

      @Override
      public void flush() throws IOException {
        outputStream.flush();
      }

      @Override
      public void close() throws IOException {
        outputStream.close();
      }
    }
  }

}
