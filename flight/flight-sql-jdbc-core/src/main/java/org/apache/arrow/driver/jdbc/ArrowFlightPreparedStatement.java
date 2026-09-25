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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.apache.arrow.driver.jdbc.client.ArrowFlightSqlClientHandler;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.util.Preconditions;
import org.apache.calcite.avatica.AvaticaPreparedStatement;
import org.apache.calcite.avatica.Meta.Signature;
import org.apache.calcite.avatica.Meta.StatementHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Arrow Flight JBCS's implementation {@link PreparedStatement}. */
public class ArrowFlightPreparedStatement extends AvaticaPreparedStatement
    implements ArrowFlightInfoStatement {

  private static final Logger LOGGER = LoggerFactory.getLogger(ArrowFlightPreparedStatement.class);

  private final ArrowFlightSqlClientHandler.PreparedStatement preparedStatement;

  /** FG patch: cancel credential captured from the first poll of the poll-based execute. */
  private volatile FlightInfo cancelCredential;

  private ArrowFlightPreparedStatement(
      final ArrowFlightConnection connection,
      final ArrowFlightSqlClientHandler.PreparedStatement preparedStmt,
      final StatementHandle handle,
      final Signature signature,
      final int resultSetType,
      final int resultSetConcurrency,
      final int resultSetHoldability)
      throws SQLException {
    super(connection, handle, signature, resultSetType, resultSetConcurrency, resultSetHoldability);
    this.preparedStatement = Preconditions.checkNotNull(preparedStmt);
  }

  static ArrowFlightPreparedStatement newPreparedStatement(
      final ArrowFlightConnection connection,
      final ArrowFlightSqlClientHandler.PreparedStatement preparedStmt,
      final StatementHandle statementHandle,
      final Signature signature,
      final int resultSetType,
      final int resultSetConcurrency,
      final int resultSetHoldability)
      throws SQLException {
    return new ArrowFlightPreparedStatement(
        connection,
        preparedStmt,
        statementHandle,
        signature,
        resultSetType,
        resultSetConcurrency,
        resultSetHoldability);
  }

  @Override
  public ArrowFlightConnection getConnection() throws SQLException {
    return (ArrowFlightConnection) super.getConnection();
  }

  @Override
  public synchronized void close() throws SQLException {
    this.preparedStatement.close();
    super.close();
  }

  @Override
  public FlightInfo executeFlightInfoQuery() throws SQLException {
    try {
      return preparedStatement.executeQuery(this::recordCancelCredential);
    } catch (final FlightRuntimeException e) {
      // FG patch: surface poll-loop outcomes (CANCELLED, TIMED_OUT, ...) as SQLException.
      throw new SQLException("Query execution failed.", e);
    }
  }

  private void recordCancelCredential(final FlightInfo info) {
    this.cancelCredential = info;
  }

  /**
   * FG patch: while {@code executeQuery} is blocked polling, the Avatica-level cancel is a no-op
   * (no result set exists yet), so forward CancelFlightInfo with the credential captured from the
   * first poll before falling back to the standard behavior. Best effort: a server may not
   * implement CancelFlightInfo or report the query not cancelable.
   *
   * <p>Must not be {@code synchronized}, and {@code super.cancel()} is skipped before the
   * credential exists (see {@link ArrowFlightStatement#cancel()}).
   */
  @Override
  public void cancel() throws SQLException {
    final FlightInfo credential = cancelCredential;
    if (credential != null) {
      try {
        getConnection().getClientHandler().cancelFlightInfo(credential);
      } catch (final RuntimeException e) {
        LOGGER.debug("Suppressed CancelFlightInfo failure during cancel", e);
      }
      super.cancel();
    } else {
      checkOpen();
      cancelFlag.set(true);
    }
  }
}
