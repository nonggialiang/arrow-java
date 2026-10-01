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
import java.sql.Statement;
import org.apache.arrow.flight.FlightInfo;

/** A {@link Statement} that deals with {@link FlightInfo}. */
public interface ArrowFlightInfoStatement extends Statement {

  @Override
  ArrowFlightConnection getConnection() throws SQLException;

  /**
   * Executes the query this {@link Statement} is holding.
   *
   * @return the {@link FlightInfo} for the results.
   * @throws SQLException on error.
   */
  FlightInfo executeFlightInfoQuery() throws SQLException;

  /**
   * FG patch (D27): whether this statement was created with a scrollable result set type ({@code
   * TYPE_SCROLL_INSENSITIVE}, or {@code TYPE_SCROLL_SENSITIVE} downgraded to it). Scroll statements
   * register with the FG gateway header {@code x-fg-result-set-type: scroll} and get a server-side
   * paged (random access) ResultSet instead of the default streaming one.
   */
  boolean isScrollable();

  /**
   * FG patch (D28): DBeaver segment 对齐的页大小提示（statementDefaultScroll 连接上由 executeFlightInfoQuery
   * 推导：fresh = maxRows / 续传 = maxRows - lastMaxRows）。 0 = 无提示（ScrollResultSet 回落 fetchSize/网关默认）。
   */
  default int pageSizeHint() {
    return 0;
  }
}
