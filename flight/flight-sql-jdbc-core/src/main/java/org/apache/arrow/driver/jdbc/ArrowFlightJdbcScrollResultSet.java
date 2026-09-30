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
import java.util.Calendar;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import org.apache.arrow.driver.jdbc.utils.ConvertUtils;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.TransferPair;
import org.apache.calcite.avatica.AvaticaResultSet;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta.Frame;
import org.apache.calcite.avatica.Meta.Signature;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.util.AbstractCursor;
import org.apache.calcite.avatica.util.ArrayImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FG patch (D27): server-side paged {@code TYPE_SCROLL_INSENSITIVE} ResultSet.
 *
 * <p>Instead of draining the whole result into client memory, the scroll statement registers at the
 * FG gateway with {@code x-fg-result-set-type: scroll} and receives a single STREAM-ticket
 * endpoint; this ResultSet then fetches <b>pages</b> on demand by re-presenting that ticket on
 * DoGet with {@code x-fg-page-offset}/{@code x-fg-page-limit} headers (the gateway slices rows
 * server-side over materialized results). Positioning ({@code next/absolute/relative/first/last/
 * previous/beforeFirst/afterLast}) is computed locally against {@code FlightInfo.getRecordCount()}
 * and mapped onto page-aligned fetches; a small LRU of page batches backs re-reads and backward
 * jumps. Page size = {@code Statement.getFetchSize()} (default 1000, matching the gateway's {@code
 * fg.result.page.default-rows}).
 */
public class ArrowFlightJdbcScrollResultSet extends AvaticaResultSet {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ArrowFlightJdbcScrollResultSet.class);

  private static final int DEFAULT_PAGE_SIZE = 1000;
  private static final int PAGE_CACHE_LIMIT = 8;

  private final ArrowFlightInfoStatement statement;
  private final byte[] ticketBytes;
  private final long totalRows;
  private final int pageSize;

  /** LRU of fetched pages keyed by page start offset; eviction closes the batch. */
  private final LinkedHashMap<Long, ArrowRecordBatch> pageCache =
      new LinkedHashMap<Long, ArrowRecordBatch>(16, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(final Map.Entry<Long, ArrowRecordBatch> eldest) {
          if (size() > PAGE_CACHE_LIMIT) {
            eldest.getValue().close();
            return true;
          }
          return false;
        }
      };

  /** The single root accessors are bound to; the current page is (re)loaded into it. */
  private VectorSchemaRoot root;

  private VectorLoader loader;
  private long loadedPageStart = Long.MIN_VALUE;

  /** Cursor position: global 0-based row index, -1 = before first. */
  private long pos = -1;

  ArrowFlightJdbcScrollResultSet(
      final AvaticaStatement statement,
      final QueryState state,
      final Signature signature,
      final ResultSetMetaData resultSetMetaData,
      final TimeZone timeZone,
      final Frame firstFrame,
      final FlightInfo info)
      throws SQLException {
    super(statement, state, signature, resultSetMetaData, timeZone, firstFrame);
    this.statement = (ArrowFlightInfoStatement) statement;
    if (info.getEndpoints().isEmpty() || info.getEndpoints().get(0).getTicket() == null) {
      throw new SQLException("Scrollable execution returned no ticket endpoint");
    }
    this.ticketBytes = info.getEndpoints().get(0).getTicket().getBytes();
    this.totalRows = Math.max(0L, info.getRecords());
    int fetchSize;
    try {
      fetchSize = statement.getFetchSize();
    } catch (Exception e) {
      fetchSize = 0;
    }
    this.pageSize = fetchSize > 0 ? fetchSize : DEFAULT_PAGE_SIZE;
    this.schema = info.getSchemaOptional().orElse(null);
  }

  private final Schema schema;

  /**
   * Avatica 生命周期（同 ArrowFlightJdbcFlightStreamResultSet）：构造器只捕获执行结果， 数据装配延迟到本方法——由 Avatica 的
   * PrepareCallback 在 execute() 时调用。覆盖基类 默认实现（其经 cursorFactory 路径，Arrow 签名的 cursorFactory 恒为 null 会
   * NPE）。
   */
  @Override
  protected AvaticaResultSet execute() throws SQLException {
    if (schema != null) {
      final List<ColumnMetaData> columns =
          ConvertUtils.convertArrowFieldsToColumnMetaDataList(schema.getFields());
      signature.columns.clear();
      signature.columns.addAll(columns);
      this.root = VectorSchemaRoot.create(schema, getAllocator());
      this.loader = new VectorLoader(root);
      execute2(new PageCursor(), this.signature.columns);
    }
    return this;
  }

  // ------------------------------------------------------------- 定位语义（自管）

  @Override
  public boolean next() throws SQLException {
    checkOpen();
    return seek(pos + 1);
  }

  @Override
  public boolean previous() throws SQLException {
    checkOpen();
    return seek(pos - 1);
  }

  @Override
  public boolean absolute(int row) throws SQLException {
    checkOpen();
    if (row == 0) {
      beforeFirst();
      return false;
    }
    final long target = row > 0 ? row - 1 : totalRows + row;
    return seek(target);
  }

  @Override
  public boolean relative(int rows) throws SQLException {
    checkOpen();
    return seek(pos + rows);
  }

  @Override
  public boolean first() throws SQLException {
    checkOpen();
    return seek(0);
  }

  @Override
  public boolean last() throws SQLException {
    checkOpen();
    return totalRows == 0 ? (pos = -1) == -1 : seek(totalRows - 1);
  }

  @Override
  public void beforeFirst() throws SQLException {
    checkOpen();
    pos = -1;
  }

  @Override
  public void afterLast() throws SQLException {
    checkOpen();
    pos = totalRows;
  }

  @Override
  public boolean isBeforeFirst() throws SQLException {
    checkOpen();
    return totalRows > 0 && pos < 0;
  }

  @Override
  public boolean isAfterLast() throws SQLException {
    checkOpen();
    return totalRows > 0 && pos >= totalRows;
  }

  @Override
  public boolean isFirst() throws SQLException {
    checkOpen();
    return totalRows > 0 && pos == 0;
  }

  @Override
  public boolean isLast() throws SQLException {
    checkOpen();
    return totalRows > 0 && pos == totalRows - 1;
  }

  @Override
  public int getRow() throws SQLException {
    checkOpen();
    return pos < 0 || pos >= totalRows ? 0 : (int) (pos + 1);
  }

  /** 定位到全局行（0 基）：越界→状态置位返回 false；页未装载则取页。 */
  private boolean seek(final long target) throws SQLException {
    if (target < 0) {
      pos = -1;
      return false;
    }
    if (target >= totalRows) {
      pos = totalRows;
      return false;
    }
    ensurePage(target);
    pos = target;
    return true;
  }

  private void ensurePage(final long globalRow) throws SQLException {
    final long pageStart = (globalRow / pageSize) * pageSize;
    if (pageStart == loadedPageStart) {
      return;
    }
    ArrowRecordBatch batch = pageCache.get(pageStart);
    if (batch == null) {
      batch = fetchPage(pageStart);
      pageCache.put(pageStart, batch);
    }
    loader.load(batch);
    loadedPageStart = pageStart;
  }

  /** DoGet(STREAM 票) + 页头 → 整页装为一个 batch（网关单页单 batch 契约）。 */
  private ArrowRecordBatch fetchPage(final long pageStart) throws SQLException {
    final CallHeaders headers = new FlightCallHeaders();
    headers.insert("x-fg-page-offset", Long.toString(pageStart));
    headers.insert("x-fg-page-limit", Integer.toString(pageSize));
    try (FlightStream stream =
        statement
            .getConnection()
            .getClientHandler()
            .getStream(new Ticket(ticketBytes), new HeaderCallOption(headers))) {
      final VectorSchemaRoot pageRoot = stream.getRoot();
      long expectedEnd = Math.min(pageStart + pageSize, totalRows);
      long got = 0;
      // 防御：页流可能多 batch（旧网关），逐段聚合到首个 root
      VectorSchemaRoot accumulator = null;
      while (stream.next()) {
        final int rows = pageRoot.getRowCount();
        if (rows == 0) {
          continue;
        }
        if (accumulator == null) {
          accumulator = VectorSchemaRoot.create(stream.getSchema(), getAllocator());
        }
        appendRows(pageRoot, accumulator);
        got += rows;
      }
      if (accumulator == null) {
        // 空页（offset ≥ total 的防御路径或 total 漂移）
        accumulator =
            VectorSchemaRoot.create(
                stream.getSchema() == null ? root.getSchema() : stream.getSchema(), getAllocator());
      } else if (got != expectedEnd - pageStart) {
        LOGGER.warn(
            "Scroll page mismatch: query row count may have drifted (expected {}, got {})",
            expectedEnd - pageStart,
            got);
      }
      final ArrowRecordBatch batch = new VectorUnloader(accumulator).getRecordBatch();
      accumulator.close();
      return batch;
    } catch (final Exception e) {
      throw new SQLException("Failed to fetch scroll page at offset " + pageStart, e);
    }
  }

  private void appendRows(final VectorSchemaRoot src, final VectorSchemaRoot dst) {
    final int base = dst.getRowCount();
    final int rows = src.getRowCount();
    for (final org.apache.arrow.vector.types.pojo.Field field : src.getSchema().getFields()) {
      final FieldVector from = src.getVector(field.getName());
      final FieldVector to = dst.getVector(field.getName());
      final TransferPair tp = from.makeTransferPair(to);
      for (int i = 0; i < rows; i++) {
        tp.copyValueSafe(i, base + i);
      }
    }
    dst.setRowCount(base + rows);
  }

  private org.apache.arrow.memory.BufferAllocator getAllocator() {
    try {
      return ((ArrowFlightConnection) statement.getConnection()).getAllocator();
    } catch (final java.sql.SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    for (final ArrowRecordBatch batch : pageCache.values()) {
      try {
        batch.close();
      } catch (final Exception e) {
        LOGGER.debug("Suppressed page close failure", e);
      }
    }
    pageCache.clear();
    if (root != null) {
      root.close();
    }
    super.close();
  }

  // ------------------------------------------------------------- 页游标

  /** 绑定持久 root 的页游标：accessor 读行索引 = 全局行 − 当前页起点。 */
  private final class PageCursor extends AbstractCursor {

    @Override
    public List<Accessor> createAccessors(
        final List<ColumnMetaData> columns,
        final Calendar localCalendar,
        final ArrayImpl.Factory factory) {
      final List<FieldVector> fieldVectors = root.getFieldVectors();
      final java.util.ArrayList<Accessor> out = new java.util.ArrayList<>(fieldVectors.size());
      for (final FieldVector vector : fieldVectors) {
        out.add(
            org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessorFactory.createAccessor(
                vector, this::getCurrentRow, (boolean wasNull) -> this.wasNull[0] = wasNull));
      }
      return out;
    }

    @Override
    protected Getter createGetter(int column) {
      throw new UnsupportedOperationException("Not allowed.");
    }

    @Override
    public boolean next() {
      return seekQuietly(pos + 1);
    }

    private boolean seekQuietly(final long target) {
      if (target < 0 || target >= totalRows) {
        if (target >= totalRows) {
          pos = totalRows;
        }
        return false;
      }
      try {
        ensurePage(target);
      } catch (final SQLException e) {
        throw new RuntimeException(e);
      }
      pos = target;
      return true;
    }

    @Override
    public void close() {
      // root 由 ResultSet.close() 释放
    }

    int getCurrentRow() {
      return (int) (pos - loadedPageStart);
    }
  }
}
