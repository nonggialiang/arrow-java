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

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Uniform view over the data of a single {@link org.apache.arrow.flight.FlightEndpoint}.
 *
 * <p>FG patch: endpoints may be backed either by a gRPC Flight stream ({@link GrpcEndpointStream})
 * or by an HTTP(S) location consumed through an {@link org.apache.arrow.vector.ipc.ArrowReader}
 * ({@link ReaderEndpointStream}); result sets and queues consume both through this interface.
 */
public interface EndpointStream extends AutoCloseable {

  /** Blocking request to load the next batch. */
  boolean next();

  /** Get the current root; its contents change on every successful {@link #next()}. */
  VectorSchemaRoot getRoot();

  /** Get the schema of this stream. May block until the schema is available. */
  Schema getSchema();

  /**
   * Cancel the underlying stream. No-op for implementations that cannot be cancelled (e.g. plain
   * HTTP readers, whose release happens on {@link #close()}).
   */
  void cancel(String message, Throwable exception);

  /** Release the underlying stream and any buffered data. */
  @Override
  void close();
}
