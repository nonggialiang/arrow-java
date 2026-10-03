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
package org.apache.arrow.driver.jdbc;

import java.sql.SQLException;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;

/**
 * FG patch (fg-p5/D30): a poll/execution failure that is <b>terminal for the query</b> —
 * retrying cannot change the outcome (query cancelled at the gateway, query failed at the
 * engine, bad SQL/credentials). Routed to the application <b>without</b> Avatica's
 * NoSuchStatementException retry loop; everything else (UNAVAILABLE, DEADLINE_EXCEEDED,
 * client poll-budget TIMED_OUT, UNKNOWN, ...) stays transient and keeps the retry, which
 * re-sends the same x-fg-query-id so the gateway ① reattaches to the in-flight row
 * (connection blip = resume, not engine re-run).
 *
 * <p>Classification source (gateway contract, FgFlightProducer): CANCELLED "Query was
 * cancelled"; FAILED as INTERNAL with description prefix "Query failed:". Other INTERNAL
 * causes (transport stack noise) are conservatively transient — a misclassified terminal
 * failure still cannot resurrect anything: the retry reuses the sticky nonce and ① keeps
 * returning the terminal row until the retry budget burns out.
 */
public class FgTerminalQueryException extends SQLException {
  private static final long serialVersionUID = 1L;

  FgTerminalQueryException(String reason, Throwable cause) {
    super(reason, cause);
  }

  /** Whether the given poll/execution failure is terminal for the query (no-retry class). */
  static boolean isTerminalOutcome(FlightRuntimeException e) {
    final FlightStatusCode code = e.status().code();
    if (code == FlightStatusCode.CANCELLED
        || code == FlightStatusCode.INVALID_ARGUMENT
        || code == FlightStatusCode.UNAUTHENTICATED
        || code == FlightStatusCode.UNAUTHORIZED) {
      return true;
    }
    return code == FlightStatusCode.INTERNAL
        && e.status().description() != null
        && e.status().description().startsWith("Query failed:");
  }
}
