/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.driver.jdbc.client;

import java.io.IOException;
import java.io.UncheckedIOException;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * A {@link EndpointStream} backed by an {@link ArrowReader}, e.g. an endpoint fetched over HTTP(S)
 * via {@code FlightSqlClient.openEndpoint}. Cancel is a no-op: releasing the reader (and the
 * underlying HTTP response body) happens on {@link #close()}.
 */
final class ReaderEndpointStream implements EndpointStream {
  private final ArrowReader reader;

  ReaderEndpointStream(ArrowReader reader) {
    this.reader = reader;
  }

  @Override
  public boolean next() {
    try {
      return reader.loadNextBatch();
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public VectorSchemaRoot getRoot() {
    try {
      return reader.getVectorSchemaRoot();
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public Schema getSchema() {
    return getRoot().getSchema();
  }

  @Override
  public void cancel(String message, Throwable exception) {
    // No transport-level cancellation for plain HTTP readers; close() aborts the body.
  }

  @Override
  public void close() {
    try {
      reader.close();
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
