package org.apache.rya.indexing.pcj.fluo.app.observers;
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
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.observer.AbstractObserver;
import org.apache.log4j.Logger;
import org.apache.rya.accumulo.utils.VisibilitySimplifier;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.indexing.pcj.fluo.app.RyaStatementSerializer;
import org.apache.rya.indexing.pcj.fluo.app.export.IncrementalBindingSetExporterFactory.IncrementalExporterFactoryException;
import org.apache.rya.indexing.pcj.fluo.app.export.IncrementalRyaStatementExporter;
import org.apache.rya.indexing.pcj.fluo.app.export.IncrementalRyaStatementExporterFactory;
import org.apache.rya.indexing.pcj.fluo.app.export.rya.RyaStatementExporterFactory;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

/**
 * Monitors the Column {@link FluoQueryColumns#CONSTRUCT_STATEMENTS} for new Construct
 * Query {@link RyaStatement}s and exports the results using the {@link IncrementalRyaStatementExporter}s
 * that are registered with this Observer.
 *
 */
public class ConstructQueryResultObserver extends AbstractObserver {

    private static final Logger log = Logger.getLogger(ConstructQueryResultObserver.class);
    
    /**
     * Simplifies Visibility expressions prior to exporting results.
     */
    private static final VisibilitySimplifier SIMPLIFIER = new VisibilitySimplifier();

    /**
     * We expect to see the same expressions a lot, so we cache the simplified forms.
     */
    private final Map<String, String> simplifiedVisibilities = new HashMap<>();

    /**
     * Builders for each type of result exporter we support.
     */
    private static final ImmutableSet<IncrementalRyaStatementExporterFactory> factories =
            ImmutableSet.<IncrementalRyaStatementExporterFactory>builder()
                .add(new RyaStatementExporterFactory())
                .build();

    /**
     * The exporters that are configured.
     */
    private ImmutableSet<IncrementalRyaStatementExporter> exporters = null;

    /**
     * Before running, determine which exporters are configured and set them up.
     */
    @Override
    public void init(final Context context) {
        final ImmutableSet.Builder<IncrementalRyaStatementExporter> exportersBuilder = ImmutableSet.builder();

        for(final IncrementalRyaStatementExporterFactory builder : factories) {
            try {
                log.debug("ConstructQueryResultObserver.init(): for each exportersBuilder=" + builder);

                final Optional<IncrementalRyaStatementExporter> exporter = builder.build(context);
                if(exporter.isPresent()) {
                    exportersBuilder.add(exporter.get());
                }
            } catch (final IncrementalExporterFactoryException e) {
                log.error("Could not initialize a result exporter.", e);
            }
        }

        exporters = exportersBuilder.build();
    }

    
    
    @Override
    public ObservedColumn getObservedColumn() {
        return new ObservedColumn(FluoQueryColumns.CONSTRUCT_STATEMENTS, NotificationType.STRONG);
    }

    @Override
    public void process(TransactionBase tx, Bytes row, Column col) throws Exception {
        Bytes bytes = tx.get(row,col);
        RyaStatement statement = RyaStatementSerializer.deserialize(bytes.toArray());
        statement = simplifyVisibilities(statement);
        
        for(IncrementalRyaStatementExporter exporter: exporters) {
            exporter.export(row.toString(), statement);
        }
    }

    private RyaStatement simplifyVisibilities(RyaStatement statement) throws UnsupportedEncodingException {
        // Simplify the result's visibilities and cache new simplified visibilities
        byte[] visibilityBytes = statement.getColumnVisibility();
        String visibility = new String(visibilityBytes, "UTF-8");
        if(!simplifiedVisibilities.containsKey(visibility)) {
            String simplified = SIMPLIFIER.simplify(visibility);
            simplifiedVisibilities.put(visibility, simplified);
        }
        statement.setColumnVisibility(simplifiedVisibilities.get(visibility).getBytes("UTF-8"));
        return statement;
    }

}
