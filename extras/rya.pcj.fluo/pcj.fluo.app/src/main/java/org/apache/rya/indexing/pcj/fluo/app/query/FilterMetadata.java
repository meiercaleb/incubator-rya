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
 */
package org.apache.rya.indexing.pcj.fluo.app.query;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Optional;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.rya.indexing.pcj.fluo.app.util.FilterSerializer;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;

import com.google.common.base.Objects;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import net.jcip.annotations.Immutable;

/**
 * Metadata that is specific to Filter nodes.
 */
@Immutable
@DefaultAnnotation(NonNull.class)
public class FilterMetadata extends StateNodeMetadata {

    private final String filterSparql;
    private final String parentNodeId;
    private final String childNodeId;

    /**
     * Constructs an instance of {@link FilterMetadata}.
     *
     * @param nodeId - The ID the Fluo app uses to reference this node. (not null)
     * @param varOrder - The variable order of binding sets that are emitted by this node. (not null)
     * @param stateMetadata - Optional containing information about the aggregation state that this node depends on. (not null)
     * @param filterSparql - SPARQL query representing the filter as generated by {@link FilterSerializer#serialize}. (not null)
     * @param filterIndexWithinSparql - The index of the filter within the original SPARQL query
     *   that this node processes. (not null)
     * @param parentNodeId - The node id of this node's parent. (not null)
     * @param childNodeId - The node id of this node's child. (not null)
     */
    public FilterMetadata(
            final String nodeId,
            final VariableOrder varOrder,
            final Optional<CommonNodeMetadataImpl> state,
            final String filterSparql,
            final String parentNodeId,
            final String childNodeId) {
        super(nodeId, varOrder, state);
        this.filterSparql = checkNotNull(filterSparql);
        this.parentNodeId = checkNotNull(parentNodeId);
        this.childNodeId = checkNotNull(childNodeId);
    }

    /**
     * @return The original SPARQL query the filter is derived from.
     */
    public String getFilterSparql() {
        return filterSparql;
    }

    /**
     * @return The node id of this node's parent.
     */
    public String getParentNodeId() {
        return parentNodeId;
    }

    /**
     * @return The node whose results are being filtered.
     */
    public String getChildNodeId() {
        return childNodeId;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(
                super.getNodeId(),
                super.getVariableOrder(),
                super.getStateMetadata(),
                filterSparql,
                parentNodeId,
                childNodeId);
    }

    @Override
    public boolean equals(final Object o) {
        if(this == o) {
            return true;
        }

        if(o instanceof FilterMetadata) {
            if(super.equals(o)) {
                final FilterMetadata filterMetadata = (FilterMetadata)o;
                return new EqualsBuilder()
                        .append(filterSparql, filterMetadata.filterSparql)
                        .append(parentNodeId, filterMetadata.parentNodeId)
                        .append(childNodeId, filterMetadata.childNodeId)
                        .isEquals();
            }
            return false;

        }

        return false;
    }

    @Override
    public String toString() {
        return new StringBuilder()
                .append("Filter Metadata {\n")
                .append("    Node ID: " + super.getNodeId() + "\n")
                .append("    Variable Order: " + super.getVariableOrder() + "\n")
                .append("    State Metadata: " + super.getStateMetadata() + "\n")
                .append("    Parent Node ID: " + parentNodeId + "\n")
                .append("    Child Node ID: " + childNodeId + "\n")
                .append("    Original SPARQL: " + filterSparql + "\n")
                .append("}")
                .toString();
    }

    /**
     * Creates a new {@link Builder} for this class.
     *
     * @param nodeId - The ID the Fluo app uses to reference this node. (not null)
     * @return A new {@link Builder} for this class.
     */
    public static Builder builder(final String nodeId) {
        return new Builder(nodeId);
    }

    /**
     * Builds instances of {@link FilterMetadata}.
     */
    @DefaultAnnotation(NonNull.class)
    public static final class Builder implements CommonNodeMetadata.Builder{

        private final String nodeId;
        private VariableOrder varOrder;
        private CommonNodeMetadataImpl state;
        private String filterSparql;
        private String parentNodeId;
        private String childNodeId;

        /**
         * Constructs an instance of {@link Builder}.
         *
         * @param nodeId - the ID the Fluo app uses to reference this node.
         */
        public Builder(final String nodeId) {
            this.nodeId = checkNotNull(nodeId);
        }

        /**
         * @return the ID the Fluo app uses to reference this node.
         */
        public String getNodeId() {
            return nodeId;
        }

        /**
         * Set the variable order of binding sets that are emitted by this node.
         *
         * @param varOrder - The variable order of binding sets that are emitted by this node.
         * @return This builder so that method invocations may be chained.
         */
        public Builder setVarOrder(@Nullable final VariableOrder varOrder) {
            this.varOrder = varOrder;
            return this;
        }
        
        @Override
        public VariableOrder getVariableOrder() {
            return varOrder;
        }

        /**
         * Set the original SPARQL query the filter is derived from.
         *
         * @param originalSparql - The original SPARQL query the filter is derived from.
         * @return This builder so that method invocations may be chained.
         */
        public Builder setFilterSparql(final String originalSparql) {
            this.filterSparql = originalSparql;
            return this;
        }

        /**
         * Set the node ID of this node's parent.
         *
         * @param parentNodeId - The node ID of this node's parent.
         * @return This builder so that method invocations may be chained.
         */
        public Builder setParentNodeId(@Nullable final String parentNodeId) {
            this.parentNodeId = parentNodeId;
            return this;
        }

        /**
         * Set the node ID of this node's child.
         *
         * @param childNodeId - The node id of this node's child.
         * @return This builder so that method invocations may be chained.
         */
        public Builder setChildNodeId(@Nullable final String childNodeId) {
            this.childNodeId = childNodeId;
            return this;
        }
        
        public String getChildNodeId() {
            return childNodeId;
        }
        
        /**
         * Sets the Aggregation State for this Filter node.
         * @param state - Aggregation State indicating current value of Aggregation 
         * @return This builder so that method invocations may be chained. 
         */
        public Builder setStateMetadata(CommonNodeMetadataImpl state) {
            this.state = state;
            return this;
        }
        
        public Optional<CommonNodeMetadataImpl> getStateMetadata() {
            return Optional.ofNullable(state);
        }

        /**
         * @return Returns an instance of {@link FilterMetadata} using this builder's values.
         */
        public FilterMetadata build() {
            return new FilterMetadata(
                    nodeId,
                    varOrder,
                    Optional.ofNullable(state),
                    filterSparql,
                    parentNodeId,
                    childNodeId);
        }
    }
}