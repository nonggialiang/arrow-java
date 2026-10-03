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
package org.apache.arrow.fg.sample;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;

/**
 * FG (flight-gateway) JDBC client sample (D29, simplified by fg-p5/D30).
 *
 * <p>用 FG 定制驱动（本 fork，{@code 19.0.0-fg-p1}）连接 flight-gateway 执行 Spark SQL：
 *
 * <ol>
 *   <li>{@code CREATE OR REPLACE TEMPORARY VIEW ... USING parquet OPTIONS (path 's3a://...')}
 *       —— 走网关命令路径；临时 view 是<b>会话级</b>对象，两个模式共用同一条连接。
 *   <li>模式一（流式）：默认连接（无参 statement = {@code TYPE_FORWARD_ONLY}），
 *       {@code next()} 逐行消费到底——单次 DoGet 中继流，统计首行耗时与「第一条消费到
 *       最后一条」的整体耗时。
 *   <li>模式二（随机分页）：同一默认连接上显式 {@code TYPE_SCROLL_INSENSITIVE} statement
 *       <b>重执行</b>同一 SELECT——fg-p5 起默认连接每次应用级 execute 自动铸造 fresh nonce
 *       {@code x-fg-query-id}（与模式一必然不同 id，网关 ② 直建 scrollable 新行、不走同
 *       session 同 SQL 指纹复用，引擎真重跑）+ 服务端页切片（D27）；按模式一行数随机页序
 *       {@code absolute(offset)} 跳页，每页 fetchSize 行，统计重执行耗时、平均每页耗时与
 *       整体耗时；首列校验和与模式一比对（页序无关的求和），验证随机页数据一致。
 * </ol>
 *
 * <p><b>历史注记</b>：fg-p5 之前默认连接不带 queryId 头，重执行同 SQL 命中网关指纹终态
 * 复用（设计行为）——复用响应是首注册的 PART 扇出票，页头对 PART 票不切片，每页 DoGet
 * 拉回整个 part；当时样例靠第二条 statementDefaultScroll 连接 + URL 钉 x-fg-session-id
 * 共享会话来获得显式 queryId。fg-p5 后单连接即正解。
 *
 * <p>运行：{@code mvn -pl flight/fg-client-sample exec:java -Dexec.args="<url> <user>
 * <password> <parquetPath> <pageSize>"}，参数可省略用默认值（url 为不含额外参数的基础
 * 地址，样例自行追加 useEncryption）。
 */
public final class FgClientSample {

  private static final String VIEW_NAME = "tempview";
  private static final String SELECT_SQL = "SELECT * FROM " + VIEW_NAME;

  public static void main(String[] args) throws Exception {
    String base = arg(args, 0, "jdbc:arrow-flight://localhost:32010");
    String user = arg(args, 1, "fg");
    String password = arg(args, 2, "fg");
    String parquetPath = arg(args, 3, "s3a://ssdr-bucket/chris/test/file.parquet");
    int pageSize = Integer.parseInt(arg(args, 4, "100"));

    Properties info = new Properties();
    info.setProperty("user", user);
    info.setProperty("password", password);

    String url = withParams(base, "useEncryption=false");
    System.out.println("[fg-sample] url=" + url);
    System.out.println("[fg-sample] parquet=" + parquetPath + " pageSize=" + pageSize);

    // 临时 view 会话级：两个模式共用同一条连接（同一个 fg 会话）
    try (Connection conn = DriverManager.getConnection(url, info)) {
      createTempView(conn, parquetPath);
      StreamResult stream = streamAll(conn);
      randomPages(conn, stream, pageSize);
    }
  }

  /** Appends URL params (auto-detects the ? / &amp; separator). */
  private static String withParams(String base, String... params) {
    StringBuilder sb = new StringBuilder(base);
    for (String p : params) {
      sb.append(sb.indexOf("?") >= 0 ? '&' : '?').append(p);
    }
    return sb.toString();
  }

  // ------------------------------------------------------------------ 步骤 1

  /** CREATE OR REPLACE TEMPORARY VIEW ... USING parquet（幂等；DDL 走网关命令路径）。 */
  private static void createTempView(Connection conn, String parquetPath) throws SQLException {
    String ddl =
        "CREATE OR REPLACE TEMPORARY VIEW "
            + VIEW_NAME
            + " USING org.apache.spark.sql.parquet OPTIONS (path '"
            + parquetPath
            + "')";
    long t0 = System.nanoTime();
    try (Statement st = conn.createStatement()) {
      st.execute(ddl);
    }
    System.out.printf("[fg-sample] ① 建临时 view OK（%.0f ms）%n", ms(System.nanoTime() - t0));
  }

  // ------------------------------------------------------------------ 模式一

  /** Mode-one result: row count + first-column checksum (order-independent sum for mode-two comparison). */
  private static final class StreamResult {
    final long rows;
    final long checksum;

    StreamResult(long rows, long checksum) {
      this.rows = rows;
      this.checksum = checksum;
    }
  }

  /** Streaming mode: FORWARD_ONLY (connection default) + next() row-by-row to the end. */
  private static StreamResult streamAll(Connection conn) throws SQLException {
    System.out.println("[fg-sample] ② 流式拉取：FORWARD_ONLY 单 DoGet 中继流，next() 消费到底");
    long t0 = System.nanoTime();
    long firstRow = -1;
    long lastRow = -1;
    long rows = 0;
    long checksum = 0;
    try (Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery(SELECT_SQL)) {
      while (rs.next()) {
        if (firstRow < 0) {
          firstRow = System.nanoTime();
          printFirstRow(rs);
        }
        checksum += String.valueOf(rs.getObject(1)).hashCode();
        rows++;
        lastRow = System.nanoTime();
      }
    }
    System.out.printf("[fg-sample]   行数=%d%n", rows);
    System.out.printf(
        "[fg-sample]   首行耗时（执行等待+首批）=%.0f ms%n", firstRow < 0 ? 0D : ms(firstRow - t0));
    System.out.printf(
        "[fg-sample]   首条→末条消费耗时=%.0f ms%n",
        firstRow < 0 ? 0D : ms(lastRow - firstRow));
    System.out.printf("[fg-sample]   端到端=%.0f ms%n", ms(lastRow < 0 ? 0 : lastRow - t0));
    return new StreamResult(rows, checksum);
  }

  // ------------------------------------------------------------------ 模式二

  /**
   * Random paging: re-execute the SELECT with an explicit SCROLL_INSENSITIVE statement on the
   * same default connection (fg-p5: every app-level execute mints a fresh x-fg-query-id,
   * gateway ② builds a new scrollable row; server-side page slicing per D27), then jump
   * pages in random order via {@code absolute(offset)}.
   */
  private static void randomPages(Connection conn, StreamResult stream, int pageSize)
      throws SQLException {
    if (stream.rows <= 0) {
      System.out.println("[fg-sample] ③ 跳过：模式一无数据");
      return;
    }
    int totalPages = (int) ((stream.rows + pageSize - 1) / pageSize);
    List<Integer> order = new ArrayList<>(totalPages);
    for (int i = 0; i < totalPages; i++) {
      order.add(i);
    }
    Collections.shuffle(order, new Random());
    System.out.printf(
        "[fg-sample] ③ 随机分页：重执行 + absolute 跳页，%d 行 = %d 页 × %d（随机序）%n",
        stream.rows, totalPages, pageSize);

    try (Statement st =
        conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
      st.setFetchSize(pageSize); // 页大小对齐 fetchSize（D27 服务端页切片）
      long sumPageNanos = 0;
      long minPage = Long.MAX_VALUE;
      long maxPage = Long.MIN_VALUE;
      long gotRows = 0;
      long checksum = 0;
      long t0 = System.nanoTime();
      try (ResultSet rs = st.executeQuery(SELECT_SQL)) {
        System.out.printf(
            "[fg-sample]   重执行（引擎真跑 + 物化，D28 ② 新 queryId 直建行）=%.0f ms%n",
            ms(System.nanoTime() - t0));
        long pageStart = System.nanoTime();
        for (int i = 0; i < order.size(); i++) {
          int page = order.get(i);
          long start = System.nanoTime();
          int got = 0;
          // absolute 落在本页第一行；do/while 读满一页（末页可短）
          if (rs.absolute((int) ((long) page * pageSize + 1))) {
            do {
              checksum += String.valueOf(rs.getObject(1)).hashCode();
              got++;
              gotRows++;
            } while (got < pageSize && rs.next());
          }
          long elapsed = System.nanoTime() - start;
          sumPageNanos += elapsed;
          minPage = Math.min(minPage, elapsed);
          maxPage = Math.max(maxPage, elapsed);
          System.out.printf(
              "[fg-sample]   第 %2d/%d 次取：第 %d 页（行 %d-%d）%d 行  %.1f ms%n",
              i + 1,
              totalPages,
              page + 1,
              page * pageSize + 1,
              page * pageSize + got,
              got,
              ms(elapsed));
        }
        long wall = System.nanoTime() - pageStart;
        System.out.printf("[fg-sample]   取回行数=%d  平均每页=%.1f ms  最快=%.1f  最慢=%.1f%n",
            gotRows, ms(sumPageNanos) / totalPages, ms(minPage), ms(maxPage));
        System.out.printf(
            "[fg-sample]   翻页整体耗时（不含重执行）=%.0f ms%n", ms(wall));
        if (gotRows == stream.rows && checksum == stream.checksum) {
          System.out.println("[fg-sample]   首列校验和与模式一一致（随机页数据正确）");
        } else {
          System.out.printf(
              "[fg-sample]   !! 校验不一致：rows %d/%d checksum %d/%d%n",
              gotRows, stream.rows, checksum, stream.checksum);
        }
      }
    }
  }

  // ------------------------------------------------------------------ 工具

  private static void printFirstRow(ResultSet rs) throws SQLException {
    ResultSetMetaData md = rs.getMetaData();
    int n = md.getColumnCount();
    StringBuilder sb = new StringBuilder();
    for (int i = 1; i <= n; i++) {
      if (i > 1) {
        sb.append(" | ");
      }
      String v = String.valueOf(rs.getObject(i));
      if (v.length() > 40) {
        v = v.substring(0, 40) + "...";
      }
      sb.append(md.getColumnLabel(i)).append('=').append(v);
    }
    System.out.println("[fg-sample]   " + sb);
  }

  private static double ms(long nanos) {
    return nanos / 1_000_000.0;
  }

  private static String arg(String[] args, int i, String dflt) {
    return args.length > i ? args[i] : dflt;
  }

  private FgClientSample() {}
}
