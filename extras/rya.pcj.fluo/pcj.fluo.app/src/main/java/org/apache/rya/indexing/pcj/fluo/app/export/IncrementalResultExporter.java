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
package org.apache.rya.indexing.pcj.fluo.app.export;

import java.util.Set;

import org.apache.rya.api.client.CreatePCJ.ExportStrategy;
import org.apache.rya.api.client.CreatePCJ.QueryType;

/**
 * Common interface for the different incremental exporters used in the Rya Fluo Application.
 *
 */
public interface IncrementalResultExporter extends AutoCloseable {

    /**
     * @return - A Set of {@link QueryType}s whose results this exporter handles
     */
    public Set<QueryType> getQueryTypes();
    
    /**
     * @return - The {@link ExportStrategy} indicating where results are exported
     */
    public ExportStrategy getExportStrategy();
    
}
