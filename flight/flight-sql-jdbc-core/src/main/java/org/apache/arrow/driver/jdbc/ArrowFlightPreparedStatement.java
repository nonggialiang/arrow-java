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
    this.scrollable = resultSetType != java.sql.ResultSet.TYPE_FORWARD_ONLY;
  }

  /** FG patch (D27): scrollable statements get server-side paged result sets. */
  private final boolean scrollable;

  /** FG patch (D28): segment 对齐页大小（同 ArrowFlightStatement）。 */
  private volatile int fgPageSizeHint;

  /**
   * FG patch (fg-p5/D30)：瞬态失败续传粘槽（仅默认连接；语义同 ArrowFlightStatement——
   * PreparedStatement 路径经 meta.execute + executeQueryInternal，实证无 Avatica 重试环，
   * 粘槽在此无消费者，保留对称实现 + 入口清槽防御）。
   */
  private String fgStickyNonce;
  private String fgStickySql;

  /** 应用级 execute 边界清粘槽。 */
  private void fgClearStickyExecution() {
    fgStickyNonce = null;
    fgStickySql = null;
  }

  @Override
  public int pageSizeHint() {
    return fgPageSizeHint;
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
    if (scrollable && getResultSetConcurrency() == java.sql.ResultSet.CONCUR_UPDATABLE) {
      throw new SQLException("TYPE_SCROLL_INSENSITIVE requires CONCUR_READ_ONLY");
    }
    final ArrowFlightConnection conn = getConnection();
    final String pending = conn.pendingSql(handle);
    final String sql =
        pending != null ? pending : (getSignature() == null ? null : getSignature().sql);
    org.apache.arrow.flight.CallOption queryIdOption = null;
    int maxRows = 0;
    try {
      maxRows = getMaxRows();
    } catch (Exception ignored) {
    }
    if (conn.statementDefaultScroll() && sql != null) {
      final String continuationId = conn.queryIdForContinuation(sql, maxRows);
      final String headerId =
          continuationId != null ? continuationId : java.util.UUID.randomUUID().toString();
      final int segment =
          continuationId != null && maxRows > 0 ? maxRows - conn.lastMaxRowsSnapshot() : maxRows;
      fgPageSizeHint = segment > 0 ? Math.min(segment, 65536) : 0;
      final org.apache.arrow.flight.CallHeaders headers =
          new org.apache.arrow.flight.FlightCallHeaders();
      headers.insert("x-fg-query-id", headerId);
      queryIdOption = new org.apache.arrow.flight.HeaderCallOption(headers);
    } else if (sql != null) {
      // FG patch (fg-p5/D30)：默认连接每次应用级 execute 铸新 queryId（同 ArrowFlightStatement）
      if (fgStickyNonce == null || !sql.trim().equals(fgStickySql)) {
        fgStickyNonce = java.util.UUID.randomUUID().toString();
        fgStickySql = sql.trim();
      }
      final org.apache.arrow.flight.CallHeaders headers =
          new org.apache.arrow.flight.FlightCallHeaders();
      headers.insert("x-fg-query-id", fgStickyNonce);
      queryIdOption = new org.apache.arrow.flight.HeaderCallOption(headers);
    }
    try {
      final org.apache.arrow.flight.FlightInfo info;
      if (queryIdOption != null && scrollable) {
        info =
            preparedStatement.executeQuery(
                this::recordCancelCredential,
                ArrowFlightStatement.scrollHeaderOption(),
                queryIdOption);
      } else if (queryIdOption != null) {
        info = preparedStatement.executeQuery(this::recordCancelCredential, queryIdOption);
      } else if (scrollable) {
        info =
            preparedStatement.executeQuery(
                this::recordCancelCredential, ArrowFlightStatement.scrollHeaderOption());
      } else {
        info = preparedStatement.executeQuery(this::recordCancelCredential);
      }
      if (conn.statementDefaultScroll() && sql != null) {
        final byte[] meta = info.getAppMetadata();
        conn.recordExecution(
            sql,
            meta != null && meta.length > 0
                ? new String(meta, java.nio.charset.StandardCharsets.UTF_8)
                : null,
            maxRows);
      }
      // fg-p5 后补（D32）：成功不清粘槽（同 ArrowFlightStatement——下游取数失败仍在重试窗口）
      return info;
    } catch (final FlightRuntimeException e) {
      // FG patch (fg-p5/D30)：终态失败直达应用（同 ArrowFlightStatement）
      if (FgTerminalQueryException.isTerminalOutcome(e)) {
        fgClearStickyExecution();
        throw new FgTerminalQueryException("Query execution failed (terminal outcome).", e);
      }
      throw new SQLException("Query execution failed.", e);
    }
  }

  // ------------------------------------------------ FG patch (fg-p5/D30)：应用级 execute 入口清粘槽

  @Override
  public boolean execute() throws SQLException {
    fgClearStickyExecution();
    return super.execute();
  }

  @Override
  public java.sql.ResultSet executeQuery() throws SQLException {
    fgClearStickyExecution();
    return super.executeQuery();
  }

  @Override
  public long executeLargeUpdate() throws SQLException {
    fgClearStickyExecution();
    return super.executeLargeUpdate();
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
