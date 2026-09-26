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
package org.apache.arrow.flight.sql;

import java.util.Arrays;
import java.util.Map;
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.CancelFlightInfoRequest;
import org.apache.arrow.flight.CancelFlightInfoResult;
import org.apache.arrow.flight.CloseSessionRequest;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.GetSessionOptionsRequest;
import org.apache.arrow.flight.GetSessionOptionsResult;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.flight.PollInfo;
import org.apache.arrow.flight.SchemaResult;
import org.apache.arrow.flight.SessionOptionValue;
import org.apache.arrow.flight.SetSessionOptionsRequest;
import org.apache.arrow.flight.SetSessionOptionsResult;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.vector.ipc.ArrowReader;

/**
 * fg-p2（FG gateway）：显式 Flight SQL 会话对象——会话生命周期与 {@link FlightClient} 生命周期解耦，边界由使用者掌握。
 *
 * <p>背景：上游 {@link FlightSqlClient} 无任何会话身份机制（无自动 cookie middleware、无 id 生成、无逐请求携带钩子），而 FG 网关严格身份（无
 * cookie 无头即拒）且 session id 须为 UUID（直传引擎会话 id）。本对象在 {@link #open} 时铸造 UUID，本会话全部 RPC 自动 携带 {@code
 * x-fg-session-id} 头；{@link #close()} 发送 CloseSession DoAction，id 烧毁 （服务端 sticky CLOSED），之后须 {@code
 * open} 新会话——与网关端生命周期语义一一对应。
 *
 * <pre>{@code
 * FlightSqlSession s = FlightSqlSession.open(client, credential);
 * s.execute("SELECT 1");
 * s.setSessionOptions(Map.of("k", SessionOptionValueFactory.makeSessionOptionValue("v")));
 * s.close();                                    // id 烧毁；同一 client 上可再 open 新会话
 * }</pre>
 *
 * <p>覆盖面：FlightSqlClient 主链路（execute/prepare/getStream/getSchema/元数据族动作/ cancel）+ fg-p1 增补的 {@code
 * openEndpoint}（presigned HTTP 取数）+ 裸 {@link FlightClient} 的 legacy/poll 原语（{@code getInfo}/{@code
 * pollInfo}）。绕过本对象直呼裸 client 的请求 不带会话头（会被严格身份网关拒绝——fail loud）。
 */
public final class FlightSqlSession implements AutoCloseable {

  /** 会话身份自报头名（与 FG 网关 D20 严格身份契约一致）。 */
  static final String HEADER_FG_SESSION_ID = "x-fg-session-id";

  private final FlightClient client;
  private final FlightSqlClient sql;
  private final String sessionId;
  private final CallOption[] baseOptions; // 含 credential 等 + 会话头
  private volatile boolean closed;

  private FlightSqlSession(
      FlightClient client, FlightSqlClient sql, String sessionId, CallOption[] baseOptions) {
    this.client = client;
    this.sql = sql;
    this.sessionId = sessionId;
    this.baseOptions = baseOptions;
  }

  /**
   * 开新会话：铸造 UUID（服务端 lazy born——首个请求携带时登记）；baseOptions 为该会话 全部 RPC 的公共选项（credential 等），会话头由本对象内部追加。
   */
  public static FlightSqlSession open(FlightClient client, CallOption... baseOptions) {
    FlightCallHeaders headers = new FlightCallHeaders();
    headers.insert(HEADER_FG_SESSION_ID, java.util.UUID.randomUUID().toString());
    CallOption[] withSession = Arrays.copyOf(baseOptions, baseOptions.length + 1);
    withSession[baseOptions.length] = new HeaderCallOption(headers);
    return new FlightSqlSession(
        client, new FlightSqlClient(client), headers.get(HEADER_FG_SESSION_ID), withSession);
  }

  /** 本会话 id（UUID；close 后已烧毁，勿复用）。 */
  public String sessionId() {
    return sessionId;
  }

  public boolean isOpen() {
    return !closed;
  }

  // ------------------------------------------------------------- 查询主链路

  /** GetFlightInfo（fg-p1 patched 驱动语义：execute 为 poll-with-fallback）。 */
  public FlightInfo execute(String query, CallOption... extra) {
    return sql.execute(query, opts(extra));
  }

  /** DoGet 取数。 */
  public FlightStream getStream(Ticket ticket, CallOption... extra) {
    return sql.getStream(ticket, opts(extra));
  }

  /** plan-only GetSchema。 */
  public SchemaResult getSchema(FlightDescriptor descriptor, CallOption... extra) {
    return sql.getSchema(descriptor, opts(extra));
  }

  /** 预编译（本会话选项随包装对象的 execute/close 继续携带）。 */
  public SessionPreparedStatement prepare(String query, CallOption... extra) {
    return new SessionPreparedStatement(sql.prepare(query, opts(extra)));
  }

  /** fg-p1：presigned endpoint 的 HTTP GET 取数（空票 + http(s) location）。 */
  public ArrowReader openEndpoint(FlightEndpoint endpoint, CallOption... extra) {
    return sql.openEndpoint(endpoint, opts(extra));
  }

  /** legacy 快返原语（裸 client GetFlightInfo，STREAM 快返票路径）。 */
  public FlightInfo getInfo(FlightDescriptor descriptor, CallOption... extra) {
    return client.getInfo(descriptor, opts(extra));
  }

  /** poll 原语（裸 client PollFlightInfo）。 */
  public PollInfo pollInfo(FlightDescriptor descriptor, CallOption... extra) {
    return client.pollInfo(descriptor, opts(extra));
  }

  /** 在途取消。 */
  public CancelFlightInfoResult cancelFlightInfo(
      CancelFlightInfoRequest request, CallOption... extra) {
    return sql.cancelFlightInfo(request, opts(extra));
  }

  // ------------------------------------------------------------- 会话选项 / 生命周期

  /** SetSessionOptions（值经引擎会话 conf 即时生效；空值 = 清除）。 */
  public SetSessionOptionsResult setSessionOptions(
      Map<String, SessionOptionValue> sessionOptions, CallOption... extra) {
    return sql.setSessionOptions(new SetSessionOptionsRequest(sessionOptions), opts(extra));
  }

  /** GetSessionOptions（已登记键实时回读）。 */
  public GetSessionOptionsResult getSessionOptions(CallOption... extra) {
    return sql.getSessionOptions(new GetSessionOptionsRequest(), opts(extra));
  }

  /** 关闭会话：发送 CloseSession DoAction（服务端释放引擎会话并 sticky 关闭本 id）。 */
  @Override
  public void close() {
    if (!closed) {
      closed = true;
      sql.closeSession(new CloseSessionRequest(), baseOptions);
    }
  }

  // ------------------------------------------------------------- 内部

  private CallOption[] opts(CallOption... extra) {
    if (closed) {
      throw new IllegalStateException(
          "FlightSqlSession "
              + sessionId
              + " is closed;"
              + " open a new session (id is burned server-side)");
    }
    if (extra.length == 0) {
      return baseOptions;
    }
    CallOption[] all = Arrays.copyOf(baseOptions, baseOptions.length + extra.length);
    System.arraycopy(extra, 0, all, baseOptions.length, extra.length);
    return all;
  }

  /** PreparedStatement 薄包装：execute/close 继续携带本会话选项。 */
  public final class SessionPreparedStatement implements AutoCloseable {
    private final FlightSqlClient.PreparedStatement ps;

    private SessionPreparedStatement(FlightSqlClient.PreparedStatement ps) {
      this.ps = ps;
    }

    public FlightInfo execute(CallOption... extra) {
      return ps.execute(opts(extra));
    }

    public org.apache.arrow.vector.types.pojo.Schema getResultSetSchema() {
      return ps.getResultSetSchema();
    }

    @Override
    public void close() {
      ps.close(baseOptions);
    }
  }
}
