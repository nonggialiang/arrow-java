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

import static org.apache.arrow.driver.jdbc.utils.ArrowFlightConnectionConfigImpl.ArrowFlightConnectionProperty.replaceSemiColons;

import io.netty.util.concurrent.DefaultThreadFactory;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.arrow.driver.jdbc.client.ArrowFlightSqlClientHandler;
import org.apache.arrow.driver.jdbc.client.utils.FlightClientCache;
import org.apache.arrow.driver.jdbc.utils.ArrowFlightConnectionConfigImpl;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.util.Preconditions;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaFactory;
import org.apache.calcite.avatica.DriverVersion;

/** Connection to the Arrow Flight server. */
public final class ArrowFlightConnection extends AvaticaConnection {

  private final BufferAllocator allocator;
  private final ArrowFlightSqlClientHandler clientHandler;
  private final ArrowFlightConnectionConfigImpl config;
  private ExecutorService executorService;
  private int metadataResultSetCount;
  private Map<Integer, ArrowFlightJdbcFlightStreamResultSet> metadataResultSetMap = new HashMap<>();

  /**
   * Creates a new {@link ArrowFlightConnection}.
   *
   * @param driver the {@link ArrowFlightJdbcDriver} to use.
   * @param factory the {@link AvaticaFactory} to use.
   * @param url the URL to use.
   * @param properties the {@link Properties} to use.
   * @param config the {@link ArrowFlightConnectionConfigImpl} to use.
   * @param allocator the {@link BufferAllocator} to use.
   * @param clientHandler the {@link ArrowFlightSqlClientHandler} to use.
   */
  private ArrowFlightConnection(
      final ArrowFlightJdbcDriver driver,
      final AvaticaFactory factory,
      final String url,
      final Properties properties,
      final ArrowFlightConnectionConfigImpl config,
      final BufferAllocator allocator,
      final ArrowFlightSqlClientHandler clientHandler) {
    super(driver, factory, url, properties);
    this.config = Preconditions.checkNotNull(config, "Config cannot be null.");
    this.allocator = Preconditions.checkNotNull(allocator, "Allocator cannot be null.");
    this.clientHandler = Preconditions.checkNotNull(clientHandler, "Handler cannot be null.");
    this.metadataResultSetCount = 0;
  }

  /**
   * Creates a new {@link ArrowFlightConnection} to a {@link FlightClient}.
   *
   * @param driver the {@link ArrowFlightJdbcDriver} to use.
   * @param factory the {@link AvaticaFactory} to use.
   * @param url the URL to establish the connection to.
   * @param properties the {@link Properties} to use for this session.
   * @param allocator the {@link BufferAllocator} to use.
   * @return a new {@link ArrowFlightConnection}.
   * @throws SQLException on error.
   */
  static ArrowFlightConnection createNewConnection(
      final ArrowFlightJdbcDriver driver,
      final AvaticaFactory factory,
      String url,
      final Properties properties,
      final BufferAllocator allocator)
      throws SQLException {
    url = replaceSemiColons(url);
    final ArrowFlightConnectionConfigImpl config = new ArrowFlightConnectionConfigImpl(properties);
    final ArrowFlightSqlClientHandler clientHandler =
        createNewClientHandler(config, allocator, driver.getDriverVersion());
    return new ArrowFlightConnection(
        driver, factory, url, properties, config, allocator, clientHandler);
  }

  private static ArrowFlightSqlClientHandler createNewClientHandler(
      final ArrowFlightConnectionConfigImpl config,
      final BufferAllocator allocator,
      final DriverVersion driverVersion)
      throws SQLException {
    try {
      return new ArrowFlightSqlClientHandler.Builder()
          .withHost(config.getHost())
          .withPort(config.getPort())
          .withUsername(config.getUser())
          .withPassword(config.getPassword())
          .withTrustStorePath(config.getTrustStorePath())
          .withTrustStorePassword(config.getTrustStorePassword())
          .withSystemTrustStore(config.useSystemTrustStore())
          .withTlsRootCertificates(config.getTlsRootCertificatesPath())
          .withClientCertificate(config.getClientCertificatePath())
          .withClientKey(config.getClientKeyPath())
          .withBufferAllocator(allocator)
          .withEncryption(config.useEncryption())
          .withDisableCertificateVerification(config.getDisableCertificateVerification())
          .withToken(config.getToken())
          .withCallOptions(config.toCallOption())
          .withRetainCookies(config.retainCookies())
          .withRetainAuth(config.retainAuth())
          .withCatalog(config.getCatalog())
          .withClientCache(config.useClientCache() ? new FlightClientCache() : null)
          .withConnectTimeout(config.getConnectTimeout())
          .withDriverVersion(driverVersion)
          .withOAuthConfiguration(config.getOauthConfiguration())
          .build();
    } catch (final SQLException e) {
      try {
        allocator.close();
      } catch (final Exception allocatorCloseEx) {
        e.addSuppressed(allocatorCloseEx);
      }
      throw e;
    }
  }

  // ------------------------------------------------------------- FG patch: UI 客户端页式取数

  /**
   * FG patch（D27 后续，方案 A）：{@code statementDefaultScroll=true} 时，无参 statement 重载强制 {@code
   * TYPE_SCROLL_INSENSITIVE}——即 D27 服务端分页（注册带 x-fg-result-set-type: scroll，翻页 = 每页一个短 DoGet + 页头，驱动
   * ArrowFlightJdbcScrollResultSet 自动附页参数）。
   *
   * <p>动机：UI 客户端（DBeaver 等）读满一页即停止拉行——流式 DoGet 下这是背压，泊住 一个 relay 线程直至 readiness 超时；页式把资源模型对齐 Thrift
   * fetch（Kyuubi/Hive JDBC 无此问题的原因）：页间无在途 RPC、服务端零泊住线程、结果在对象存储里等。
   *
   * <p>边界（有意设计）：① 默认 false，未设置的连接行为零变化；② 仅升级<b>无参</b>重载 ——应用显式传 resultSetType（含
   * FORWARD_ONLY）的语句不受影响，流式自我声明仍有效； ③ 页大小 = Statement.getFetchSize()（未设 = 网关默认 1000，上限 clamp
   * 65536）； ④ 全量顺序扫描（ETL）与 https presign 直取场景<b>不建议</b>开启（页式逐页 RPC 慢于 单流；scroll 强制 relay，presign
   * 永不生效）。
   */
  @Override
  public org.apache.calcite.avatica.AvaticaStatement createStatement() throws SQLException {
    if (config.statementDefaultScroll()) {
      // Avatica 的 (int,int) 重载返回 Statement、无参重载返回 AvaticaStatement（协变）；
      // 工厂实际产出 AvaticaStatement（ArrowFlightStatement），窄化安全
      return (org.apache.calcite.avatica.AvaticaStatement)
          super.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    }
    return super.createStatement();
  }

  /** 同 {@link #createStatement()}：无参 prepareStatement 升级 scroll（见其 javadoc）。 */
  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException {
    if (config.statementDefaultScroll()) {
      return super.prepareStatement(
          sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    }
    return super.prepareStatement(sql);
  }

  void reset() throws SQLException {
    // Clean up any open Statements
    try {
      AutoCloseables.close(statementMap.values());
    } catch (final Exception e) {
      throw AvaticaConnection.HELPER.createException(e.getMessage(), e);
    }

    statementMap.clear();

    // Reset Holdability
    this.setHoldability(this.metaData.getResultSetHoldability());

    // Reset Meta
    ((ArrowFlightMetaImpl) this.meta).setDefaultConnectionProperties();
  }

  /**
   * Gets the client {@link #clientHandler} backing this connection.
   *
   * @return the handler.
   */
  /** FG patch (D27): root allocator accessor for the scrollable paged ResultSet. */
  BufferAllocator getAllocator() {
    return allocator;
  }

  ArrowFlightSqlClientHandler getClientHandler() {
    return clientHandler;
  }

  /**
   * Gets the {@link ExecutorService} of this connection.
   *
   * @return the {@link #executorService}.
   */
  synchronized ExecutorService getExecutorService() {
    return executorService =
        executorService == null
            ? Executors.newFixedThreadPool(
                config.threadPoolSize(), new DefaultThreadFactory(getClass().getSimpleName()))
            : executorService;
  }

  /**
   * Registers a new metadata ResultSet and assigns it a unique ID. Metadata ResultSets are those
   * created without an associated Statement.
   *
   * @param resultSet the ResultSet to register
   * @return the assigned ID
   */
  int getNewMetadataResultSetId(ArrowFlightJdbcFlightStreamResultSet resultSet) {
    metadataResultSetMap.put(metadataResultSetCount, resultSet);
    return metadataResultSetCount++;
  }

  /**
   * Unregisters a metadata ResultSet when it is closed. This method is called by metadata
   * ResultSets during their close operation to remove themselves from the tracking map.
   *
   * @param id the ID of the ResultSet to unregister, or null if not a metadata ResultSet
   */
  void onResultSetClose(Integer id) {
    if (id == null) {
      return;
    }
    metadataResultSetMap.remove(id);
  }

  @Override
  public Properties getClientInfo() {
    final Properties copy = new Properties();
    copy.putAll(info);
    return copy;
  }

  @Override
  public void close() throws SQLException {
    Exception topLevelException = null;
    try {
      if (executorService != null) {
        executorService.shutdown();
      }
    } catch (final Exception e) {
      topLevelException = e;
    }
    // copies of the collections are used to avoid concurrent modification problems
    ArrayList<AutoCloseable> closeables = new ArrayList<>(statementMap.values());
    closeables.addAll(new ArrayList<>(metadataResultSetMap.values()));
    closeables.add(clientHandler);
    closeables.addAll(allocator.getChildAllocators());
    closeables.add(allocator);
    try {
      AutoCloseables.close(closeables);
    } catch (final Exception e) {
      if (topLevelException == null) {
        topLevelException = e;
      } else {
        topLevelException.addSuppressed(e);
      }
    }
    try {
      super.close();
    } catch (final Exception e) {
      if (topLevelException == null) {
        topLevelException = e;
      } else {
        topLevelException.addSuppressed(e);
      }
    }
    if (topLevelException != null) {
      throw AvaticaConnection.HELPER.createException(
          topLevelException.getMessage(), topLevelException);
    }
  }

  BufferAllocator getBufferAllocator() {
    return allocator;
  }

  public ArrowFlightMetaImpl getMeta() {
    return (ArrowFlightMetaImpl) this.meta;
  }
}
