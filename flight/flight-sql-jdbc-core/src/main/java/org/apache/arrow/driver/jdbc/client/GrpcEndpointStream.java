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

import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

/** A {@link EndpointStream} backed by a gRPC {@link FlightStream}; all calls pass through. */
final class GrpcEndpointStream implements EndpointStream {
  private final FlightStream stream;

  GrpcEndpointStream(FlightStream stream) {
    this.stream = stream;
  }

  @Override
  public boolean next() {
    return stream.next();
  }

  @Override
  public VectorSchemaRoot getRoot() {
    return stream.getRoot();
  }

  @Override
  public Schema getSchema() {
    return stream.getSchema();
  }

  @Override
  public void cancel(String message, Throwable exception) {
    stream.cancel(message, exception);
  }

  @Override
  public void close() {
    try {
      stream.close();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }
}
