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

import java.sql.ResultSet;
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

  /** FG patch (D28): segment 对齐页大小（executeFlightInfoQuery 推导，ScrollResultSet 消费）。 */
  private volatile int fgPageSizeHint;

  /**
   * FG patch (fg-p5/D30；D32 修正生命周期)：瞬态失败续传粘槽（仅默认连接；
   * statementDefaultScroll 连接走 connection 级单槽）。非空 = 本次<b>应用级</b> execute
   * 尚有结果未定/未完成的尝试，Avatica 自动重试应复用同一 nonce——网关 ① 命中在途行
   * （断线续传）或已完成行（下游取数失败重试，<b>零 SQL 重跑</b>）。清槽仅两处：应用级
   * execute 入口（{@link #executeQuery(String)} 等覆写——字面语义"每次 execute 必新查询"）
   * 与终态失败（取消/引擎失败，无重试可服务）；<b>成功不清</b>——poll 成功后下游取数
   * （relay DoGet / https presign GET）失败仍属本次尝试，重试须 ① 复用而非 ② 重跑。
   */
  private String fgStickyNonce;
  private String fgStickySql;

  /** 应用级 execute 边界清粘槽（Avatica 内部重试在 executeInternal 环内直落本类，不经 JDBC 入口）。 */
  void fgClearStickyExecution() {
    fgStickyNonce = null;
    fgStickySql = null;
  }

  @Override
  public int pageSizeHint() {
    return fgPageSizeHint;
  }

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
    // FG patch（2026-10-01 DBeaver 实证）：signature 是 Avatica 的元数据增强载体，其字段
    // 赋值时序在第三方执行流（DBeaver 自己的 execute 路径）下晚于本方法触发——null 时
    // 跳过增强照常执行（原样提前返回 null 会连带跳过查询执行与 scroll 头签发，服务端
    // 侧表现为"statement 未按 scrollable 落行"）。
    final Meta.Signature signature = getSignature();

    final Schema resultSetSchema = preparedStatement.getDataSetSchema();
    if (signature != null) {
      signature.columns.addAll(
          ConvertUtils.convertArrowFieldsToColumnMetaDataList(resultSetSchema.getFields()));
      setSignature(signature);
    }

    if (scrollable && getResultSetConcurrency() == java.sql.ResultSet.CONCUR_UPDATABLE) {
      throw new SQLException("TYPE_SCROLL_INSENSITIVE requires CONCUR_READ_ONLY");
    }
    // FG patch（D28 显式续传）：statementDefaultScroll 连接上以单槽判定"段式下拉续传"
    // （同 SQL 且 maxRows 增长——DBeaver setLimit(offset, segment) 的 JDBC 泄漏信号）vs
    // "新执行"（fresh nonce → 网关 ② 直建行，queryId 即客户端铸造值）。执行后记录槽。
    final ArrowFlightConnection conn = getConnection();
    final String pending = conn.pendingSql(handle);
    final String sql = pending != null ? pending : sqlOfSignature();
    org.apache.arrow.flight.CallOption queryIdOption = null;
    int maxRows = 0;
    try {
      maxRows = getMaxRows();
    } catch (Exception ignored) {
      // Avatica 字段读取不应失败；防御
    }
    if (conn.statementDefaultScroll() && sql != null) {
      final String continuationId = conn.queryIdForContinuation(sql, maxRows);
      final String headerId =
          continuationId != null ? continuationId : java.util.UUID.randomUUID().toString();
      final int segment =
          continuationId != null && maxRows > 0 ? maxRows - lastMaxRowsFor(conn) : maxRows;
      fgPageSizeHint = segment > 0 ? Math.min(segment, 65536) : 0;
      final org.apache.arrow.flight.CallHeaders headers =
          new org.apache.arrow.flight.FlightCallHeaders();
      headers.insert("x-fg-query-id", headerId);
      queryIdOption = new org.apache.arrow.flight.HeaderCallOption(headers);
    } else if (sql != null) {
      // FG patch (fg-p5/D30)：默认连接每次应用级 execute 铸新 queryId——网关 ② 直建行，
      // 指纹去重不参与（JDBC 应用重执行同 SQL = 真重跑；DML 后重查同文本不再复用旧快照）。
      // 瞬态失败保留粘槽供 Avatica 自动重试复用（① 续传）；SQL 守卫防 ①'（粘槽换 SQL）。
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
      if (scrollable && queryIdOption != null) {
        info =
            preparedStatement.executeQuery(
                this::recordCancelCredential, scrollHeaderOption(), queryIdOption);
      } else if (scrollable) {
        info = preparedStatement.executeQuery(this::recordCancelCredential, scrollHeaderOption());
      } else if (queryIdOption != null) {
        info = preparedStatement.executeQuery(this::recordCancelCredential, queryIdOption);
      } else {
        info = preparedStatement.executeQuery(this::recordCancelCredential);
      }
      if (conn.statementDefaultScroll() && sql != null) {
        conn.recordExecution(sql, queryIdOf(info), maxRows);
      }
      // fg-p5 后补（D32）：成功不再清粘槽——清槽职责只在应用级 execute 入口与终态失败。
      // poll 成功 ≠ 本次尝试成功：下游取数失败（如 https presign URL 不可达）发生在
      // executeFlightInfoQuery 返回之后，若此刻已清槽，Avatica 重试将铸 fresh nonce →
      // 网关 ② 重新执行 SQL（实证 5 次重试 = 5 次引擎重跑）；保留粘槽则重试经 ① 复用
      // 已完成行（零重跑），只重试取数本身。
      return info;
    } catch (final FlightRuntimeException e) {
      // FG patch (fg-p5/D30)：终态失败（取消/引擎失败/SQL与认证错）直达应用——绕过
      // NoSuchStatementException 触发的 Avatica 重试环；瞬态失败保留粘槽，重试复用同
      // nonce 经网关 ① 续传在途行（断线续传而非引擎重跑）。
      if (FgTerminalQueryException.isTerminalOutcome(e)) {
        fgClearStickyExecution();
        throw new FgTerminalQueryException("Query execution failed (terminal outcome).", e);
      }
      throw new SQLException("Query execution failed.", e);
    }
  }

  // ------------------------------------------------- FG patch (fg-p5/D30)：应用级 execute 入口清粘槽
  // Avatica 的内部重试环（executeInternal catch NoSuchStatementException → resetStatement →
  // 重入）不经过这些 JDBC 入口——入口即"应用级 execute 边界"的精确判别点。

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    fgClearStickyExecution();
    return super.executeQuery(sql);
  }

  @Override
  public boolean execute(String sql) throws SQLException {
    fgClearStickyExecution();
    return super.execute(sql);
  }

  @Override
  public long executeLargeUpdate(String sql) throws SQLException {
    fgClearStickyExecution();
    return super.executeLargeUpdate(sql);
  }

  /** 槽内 lastMaxRows 只读视图（pageSize 推导用；-1 = 无槽）。 */
  private static int lastMaxRowsFor(ArrowFlightConnection conn) {
    return conn.lastMaxRowsSnapshot();
  }

  /** FlightInfo.appMetadata = queryId（FG 网关约定；缺席回落 null）。 */
  private static String queryIdOf(org.apache.arrow.flight.FlightInfo info) {
    final byte[] meta = info.getAppMetadata();
    return meta != null && meta.length > 0
        ? new String(meta, java.nio.charset.StandardCharsets.UTF_8)
        : null;
  }

  /** signature.sql 兜底（pendingSql 通道缺失时）。 */
  private String sqlOfSignature() {
    try {
      final Meta.Signature sig = getSignature();
      return sig == null ? null : sig.sql;
    } catch (Exception e) {
      return null;
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
