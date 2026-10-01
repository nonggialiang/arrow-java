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

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Properties;
import java.util.TimeZone;
import org.apache.arrow.driver.jdbc.client.ArrowFlightSqlClientHandler;
import org.apache.arrow.memory.RootAllocator;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaFactory;
import org.apache.calcite.avatica.AvaticaResultSetMetaData;
import org.apache.calcite.avatica.AvaticaSpecificDatabaseMetaData;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.UnregisteredDriver;

/** Factory for the Arrow Flight JDBC Driver. */
public class ArrowFlightJdbcFactory implements AvaticaFactory {
  private final int major;
  private final int minor;

  // This need to be public so Avatica can call this constructor
  public ArrowFlightJdbcFactory() {
    this(4, 1);
  }

  private ArrowFlightJdbcFactory(final int major, final int minor) {
    this.major = major;
    this.minor = minor;
  }

  @Override
  public AvaticaConnection newConnection(
      final UnregisteredDriver driver,
      final AvaticaFactory factory,
      final String url,
      final Properties info)
      throws SQLException {
    return ArrowFlightConnection.createNewConnection(
        (ArrowFlightJdbcDriver) driver, factory, url, info, new RootAllocator(Long.MAX_VALUE));
  }

  @Override
  public AvaticaStatement newStatement(
      final AvaticaConnection connection,
      final Meta.StatementHandle handle,
      final int resultType,
      final int resultSetConcurrency,
      final int resultSetHoldability) {
    return new ArrowFlightStatement(
        (ArrowFlightConnection) connection,
        handle,
        upgradeResultType((ArrowFlightConnection) connection, resultType),
        resultSetConcurrency,
        resultSetHoldability);
  }

  @Override
  public ArrowFlightPreparedStatement newPreparedStatement(
      final AvaticaConnection connection,
      final Meta.StatementHandle statementHandle,
      final Meta.Signature signature,
      final int resultType,
      final int resultSetConcurrency,
      final int resultSetHoldability)
      throws SQLException {
    final ArrowFlightConnection flightConnection = (ArrowFlightConnection) connection;
    ArrowFlightSqlClientHandler.PreparedStatement preparedStatement =
        flightConnection.getMeta().getPreparedStatement(statementHandle);

    return ArrowFlightPreparedStatement.newPreparedStatement(
        flightConnection,
        preparedStatement,
        statementHandle,
        signature,
        upgradeResultType(flightConnection, resultType),
        resultSetConcurrency,
        resultSetHoldability);
  }

  /**
   * FG patch（方案 A v2）：{@code statementDefaultScroll=true} 的连接上，FORWARD_ONLY （显式或默认）一律升级 {@code
   * TYPE_SCROLL_INSENSITIVE}——即 D27 服务端分页。
   *
   * <p>动机（DBeaver 实证）：UI 客户端读满一页（fetch size 恰好一个 batch）即停止 ResultSet.next()——流式 DoGet 是合法背压但泊住一个
   * relay 线程（HTTP/2 窗口不重开， 界 readiness timeout）；Thrift fetch（Kyuubi/Hive）无此问题因为逐块短 RPC + 引擎
   * buffer。页式是 Flight 原生等价物（每页一个短 DoGet，结果在对象存储里等）。v1 覆写 无参重载不够——DBeaver 执行语句经带参重载显式
   * FORWARD_ONLY；此处是 Avatica 全部 statement 创建的必经单点，覆盖所有重载。
   *
   * <p>边界：flag = 连接级策略（默认 false 零变化），逃生舱 = 不开 flag 的连接；显式 SCROLL 类型不变；页大小 = getFetchSize()（网关 clamp
   * 65536）；不建议 ETL 全量扫描 与 https presign 直取连接开启（页式逐页 RPC 慢于单流；scroll 强制 relay）。
   */
  private static int upgradeResultType(ArrowFlightConnection connection, int resultType) {
    if (connection.statementDefaultScroll() && resultType == java.sql.ResultSet.TYPE_FORWARD_ONLY) {
      return java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE;
    }
    return resultType;
  }

  @Override
  public org.apache.calcite.avatica.AvaticaResultSet newResultSet(
      final AvaticaStatement statement,
      final QueryState state,
      final Meta.Signature signature,
      final TimeZone timeZone,
      final Meta.Frame frame)
      throws SQLException {
    final ResultSetMetaData metaData = newResultSetMetaData(statement, signature);

    // FG patch (D27): scrollable statements get the server-side paged ResultSet
    if (statement instanceof ArrowFlightInfoStatement
        && ((ArrowFlightInfoStatement) statement).isScrollable()) {
      final org.apache.arrow.flight.FlightInfo info;
      try {
        info = ((ArrowFlightInfoStatement) statement).executeFlightInfoQuery();
      } catch (final SQLException e) {
        throw e;
      }
      return new ArrowFlightJdbcScrollResultSet(
          statement, state, signature, metaData, timeZone, frame, info);
    }
    return new ArrowFlightJdbcFlightStreamResultSet(
        statement, state, signature, metaData, timeZone, frame);
  }

  @Override
  public AvaticaSpecificDatabaseMetaData newDatabaseMetaData(final AvaticaConnection connection) {
    return new ArrowDatabaseMetadata(connection);
  }

  @Override
  public ResultSetMetaData newResultSetMetaData(
      final AvaticaStatement avaticaStatement, final Meta.Signature signature) {
    return new AvaticaResultSetMetaData(avaticaStatement, null, signature);
  }

  @Override
  public int getJdbcMajorVersion() {
    return major;
  }

  @Override
  public int getJdbcMinorVersion() {
    return minor;
  }
}
