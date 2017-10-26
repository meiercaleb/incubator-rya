/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */package org.apache.rya.indexing.pcj.fluo.app.query;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.concurrent.Callable;

import org.apache.fluo.api.client.SnapshotBase;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.rya.indexing.pcj.fluo.app.NodeType;

import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * Wrapper for {@link FluoQueryMetadataDAO} that caches any metadata that has
 * been retrieved from Fluo.  This class first checks the cache to see if the metadata
 * is present before delegating to the underlying DAO method to retrieve the data.
 *
 */
public class FluoQueryMetadataCache extends FluoQueryMetadataDAO{

    private final FluoQueryMetadataDAO dao;
    private static Cache<String, CommonNodeMetadata> commonNodeMetadataCache;
    private static Cache<String, Bytes> metadataCache;

    /**
     * Creates a FluoQueryMetadataCache with the specified capacity.  Old, unused results
     * are evicted as necessary.
     * @param capacity - max size of the cache
     */
    public FluoQueryMetadataCache(FluoQueryMetadataDAO dao, int capacity) {
        this.dao = dao;
        commonNodeMetadataCache = CacheBuilder.newBuilder().maximumSize(capacity).build();
        metadataCache = CacheBuilder.newBuilder().maximumSize(capacity).build();
    }

    @Override
    public StatementPatternMetadata readStatementPatternMetadata(SnapshotBase tx, String nodeId) {
        Optional<NodeType> type = NodeType.fromNodeId(nodeId);

        try {
            checkArgument(type.isPresent() && type.get() == NodeType.STATEMENT_PATTERN);
            return (StatementPatternMetadata) commonNodeMetadataCache.get(nodeId, new Callable<CommonNodeMetadata>() {
                @Override
                public CommonNodeMetadata call() throws Exception {
                    return dao.readStatementPatternMetadata(tx, nodeId);
                }
            });
        } catch (Exception e) {
            throw new RuntimeException("Unable to access StatementPatternMetadata for nodeId: " + nodeId, e);
        }
    }

    @Override
    public JoinMetadata readJoinMetadata(SnapshotBase tx, String nodeId) {
        Optional<NodeType> type = NodeType.fromNodeId(nodeId);
        try {
            checkArgument(type.isPresent() && type.get() == NodeType.JOIN);
            return (JoinMetadata) commonNodeMetadataCache.get(nodeId, new Callable<CommonNodeMetadata>() {
                @Override
                public CommonNodeMetadata call() throws Exception {
                    return dao.readJoinMetadata(tx, nodeId);
                }
            });
        } catch (Exception e) {
            throw new RuntimeException("Unable to access JoinMetadata for nodeId: " + nodeId, e);
        }
    }

    @Override
    public FilterMetadata readFilterMetadata(SnapshotBase tx, String nodeId) {
        Optional<NodeType> type = NodeType.fromNodeId(nodeId);
        try {
            checkArgument(type.isPresent() && type.get() == NodeType.FILTER);
            return (FilterMetadata) commonNodeMetadataCache.get(nodeId, new Callable<CommonNodeMetadata>() {
                @Override
                public CommonNodeMetadata call() throws Exception {
                    return dao.readFilterMetadata(tx, nodeId);
                }
            });
        } catch (Exception e) {
            throw new RuntimeException("Unable to access FilterMetadata for nodeId: " + nodeId, e);
        }
    }

    @Override
    public ProjectionMetadata readProjectionMetadata(SnapshotBase tx, String nodeId) {
        Optional<NodeType> type = NodeType.fromNodeId(nodeId);
        checkArgument(type.isPresent() && type.get() == NodeType.PROJECTION);
        try {
            return (ProjectionMetadata) commonNodeMetadataCache.get(nodeId, new Callable<CommonNodeMetadata>() {
                @Override
                public CommonNodeMetadata call() throws Exception {
                    return dao.readProjectionMetadata(tx, nodeId);
                }
            });
        } catch (Exception e) {
            throw new RuntimeException("Unable to access ProjectionMetadata for nodeId: " + nodeId, e);
        }
    }

    @Override
    public AggregationMetadata readAggregationMetadata(SnapshotBase tx, String nodeId) {
        Optional<NodeType> type = NodeType.fromNodeId(nodeId);
        try {
            checkArgument(type.isPresent() && type.get() == NodeType.AGGREGATION);
            return (AggregationMetadata) commonNodeMetadataCache.get(nodeId, new Callable<CommonNodeMetadata>() {
                @Override
                public CommonNodeMetadata call() throws Exception {
                    return dao.readAggregationMetadata(tx, nodeId);
                }
            });
        } catch (Exception e) {
            throw new RuntimeException("Unable to access AggregationMetadata for nodeId: " + nodeId, e);
        }
    }

    @Override
    public ConstructQueryMetadata readConstructQueryMetadata(SnapshotBase tx, String nodeId) {
        Optional<NodeType> type = NodeType.fromNodeId(nodeId);
        try {
            checkArgument(type.isPresent() && type.get() == NodeType.CONSTRUCT);
            return (ConstructQueryMetadata) commonNodeMetadataCache.get(nodeId, new Callable<CommonNodeMetadata>() {
                @Override
                public CommonNodeMetadata call() throws Exception {
                    return dao.readConstructQueryMetadata(tx, nodeId);
                }
            });
        } catch (Exception e) {
            throw new RuntimeException("Unable to access ConstructQueryMetadata for nodeId: " + nodeId, e);
        }
    }

    @Override
    public PeriodicQueryMetadata readPeriodicQueryMetadata(SnapshotBase tx, String nodeId) {
        Optional<NodeType> type = NodeType.fromNodeId(nodeId);
        try {
            checkArgument(type.isPresent() && type.get() == NodeType.PERIODIC_QUERY);
            return (PeriodicQueryMetadata) commonNodeMetadataCache.get(nodeId, new Callable<CommonNodeMetadata>() {
                @Override
                public CommonNodeMetadata call() throws Exception {
                    return dao.readPeriodicQueryMetadata(tx, nodeId);
                }
            });
        } catch (Exception e) {
            throw new RuntimeException("Unable to access PeriodicQueryMetadata for nodeId: " + nodeId, e);
        }
    }

    @Override
    public QueryMetadata readQueryMetadata(SnapshotBase tx, String nodeId) {
        Optional<NodeType> type = NodeType.fromNodeId(nodeId);
        try {
            checkArgument(type.isPresent() && type.get() == NodeType.QUERY);
            return (QueryMetadata) commonNodeMetadataCache.get(nodeId, new Callable<CommonNodeMetadata>() {
                @Override
                public CommonNodeMetadata call() throws Exception {
                    return dao.readQueryMetadata(tx, nodeId);
                }
            });
        } catch (Exception e) {
            throw new RuntimeException("Unable to access QueryMetadata for nodeId: " + nodeId, e);
        }
    }

    public Bytes readMetadadataEntry(SnapshotBase tx, String rowId, Column column) {
        Optional<NodeType> type = NodeType.fromNodeId(rowId);
        try {
            checkArgument(type.isPresent() && type.get().getMetaDataColumns().contains(column));
            return metadataCache.get(getKey(rowId, column), new Callable<Bytes>() {
                @Override
                public Bytes call() throws Exception {
                    return tx.get(Bytes.of(rowId), column);
                }
            });
        } catch (Exception e) {
            throw new RuntimeException("Unable to access Metadata Entry with rowId: " + rowId + " and column: " + column, e);
        }
    }

    private String getKey(String row, Column column) {
        return row + ":" + column.getsQualifier() + ":" + column.getsQualifier();
    }
}
