package org.apache.rya.indexing.pcj.fluo.app;
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
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSet;
import org.openrdf.query.algebra.StatementPattern;

import com.google.common.base.Preconditions;

/**
 * Creates a construct query graph (represented as a Set of
 * {@link RyaStatement}s with Binding names subject, predicate, object) from a
 * given BindingSet and the underlying {@link ConstructProjection}s.
 *
 */
public class ConstructGraph {


    private Set<ConstructProjection> projections;
    
    public ConstructGraph(Set<ConstructProjection> projections) {
        Preconditions.checkNotNull(projections);
        Preconditions.checkArgument(projections.size() > 0);
        this.projections = projections;
    }
    
    public ConstructGraph(Collection<StatementPattern> patterns) {
        Preconditions.checkNotNull(patterns);
        Preconditions.checkArgument(patterns.size() > 0);
        Set<ConstructProjection> projections = new HashSet<>();
        for(StatementPattern pattern: patterns) {
            projections.add(new ConstructProjection(pattern));
        }
        this.projections = projections;
    }
    
    /**
     * @return - the {@link ConstructProjection}s used to build the construct graph
     * returned by {@link ConstructGraph#createGraphFromBindingSet(VisibilityBindingSet)}.
     */
    public Set<ConstructProjection> getProjections() {
        return projections;
    }
    
    /**
     * Creates a construct query graph represented as a Set of {@link RyaStatement}s 
     * @param bs - VisiblityBindingSet used to build statement BindingSets
     * @return - Set of RyaStatements that represent a construct query graph.  
     */
    public Set<RyaStatement> createGraphFromBindingSet(VisibilityBindingSet bs) {
        Set<RyaStatement> bSets = new HashSet<>();
        for(ConstructProjection projection: projections) {
            bSets.add(projection.projectBindingSet(bs));
        }
        return bSets;
    }
    
    @Override
    public boolean equals(Object o) {
        if(this == o) {
            return true;
        }
        
        if(o instanceof ConstructGraph) {
            ConstructGraph graph = (ConstructGraph) o;
            return this.projections.equals(graph.projections);
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        int hash = 17;
        for(ConstructProjection projection: projections) {
            hash += projection.hashCode();
        }
        
        return hash;
    }
}
