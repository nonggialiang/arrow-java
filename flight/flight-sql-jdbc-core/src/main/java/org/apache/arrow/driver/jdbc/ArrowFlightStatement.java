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
import org.apache.arrow.driver.jdbc.client.ArrowFlightSqlClientHandler.PreparedStatement;
import org.apache.arrow.driver.jdbc.utils.ConvertUtils;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.Meta.StatementHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A SQL statement for querying data from an Arrow Flight server. */
public class ArrowFlightStatement extends AvaticaStatement implements ArrowFlightInfoStatement {

  private static final Logger LOGGER = LoggerFactory.getLogger(ArrowFlightStatement.class);

  /** FG patch: cancel credential captured from the first poll of the poll-based execute. */
  private volatile FlightInfo cancelCredential;

  /** FG patch (D27): scrollable statements get server-side paged result sets. */
  private final boolean scrollable;

  ArrowFlightStatement(
      final ArrowFlightConnection connection,
      final StatementHandle handle,
      final int resultSetType,
      final int resultSetConcurrency,
      final int resultSetHoldability) {
    super(connection, handle, resultSetType, resultSetConcurrency, resultSetHoldability);
    this.scrollable = resultSetType != java.sql.ResultSet.TYPE_FORWARD_ONLY;
  }

  /** FG patch (D27): SENSITIVE requests are served as INSENSITIVE (snapshot semantics). */
  @Override
  public int getResultSetType() {
    return scrollable
        ? java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE
        : java.sql.ResultSet.TYPE_FORWARD_ONLY;
  }

  @Override
  public boolean isScrollable() {
    return scrollable;
  }

  @Override
  public ArrowFlightConnection getConnection() throws SQLException {
    return (ArrowFlightConnection) super.getConnection();
  }

  @Override
  public FlightInfo executeFlightInfoQuery() throws SQLException {
    final PreparedStatement preparedStatement =
        getConnection().getMeta().getPreparedStatement(handle);
    final Meta.Signature signature = getSignature();
    if (signature == null) {
      return null;
    }

    final Schema resultSetSchema = preparedStatement.getDataSetSchema();
    signature.columns.addAll(
        ConvertUtils.convertArrowFieldsToColumnMetaDataList(resultSetSchema.getFields()));
    setSignature(signature);

    if (scrollable && getResultSetConcurrency() == java.sql.ResultSet.CONCUR_UPDATABLE) {
      throw new SQLException("TYPE_SCROLL_INSENSITIVE requires CONCUR_READ_ONLY");
    }
    try {
      return scrollable
          ? preparedStatement.executeQuery(this::recordCancelCredential, scrollHeaderOption())
          : preparedStatement.executeQuery(this::recordCancelCredential);
    } catch (final FlightRuntimeException e) {
      // FG patch: surface poll-loop outcomes (CANCELLED, TIMED_OUT, ...) as SQLException.
      throw new SQLException("Query execution failed.", e);
    }
  }

  private void recordCancelCredential(final FlightInfo info) {
    this.cancelCredential = info;
  }

  /** FG patch (D27): header marking this execution as scrollable at the gateway. */
  static org.apache.arrow.flight.CallOption scrollHeaderOption() {
    final org.apache.arrow.flight.CallHeaders headers =
        new org.apache.arrow.flight.FlightCallHeaders();
    headers.insert("x-fg-result-set-type", "scroll");
    return new org.apache.arrow.flight.HeaderCallOption(headers);
  }

  /**
   * FG patch: while {@code executeQuery} is blocked polling, the Avatica-level cancel is a no-op
   * (no result set exists yet), so forward CancelFlightInfo with the credential captured from the
   * first poll before falling back to the standard behavior. Best effort: a server may not
   * implement CancelFlightInfo or report the query not cancelable.
   *
   * <p>Must not be {@code synchronized}: the executing thread holds <em>this statement's</em>
   * monitor for the whole poll-based execute (the Avatica {@code PrepareCallback} monitor is the
   * statement itself, and {@code ArrowFlightMetaImpl} synchronizes on it around the execute).
   * Before the credential exists (the first poll has not returned yet) {@code super.cancel()} is
   * skipped altogether: the synchronized superclass method would block on that monitor until the
   * poll loop exits, preventing the caller from retrying once the credential arrives at the first
   * poll return. Its effect is replicated inline ({@code openResultSet} is null at that point).
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
