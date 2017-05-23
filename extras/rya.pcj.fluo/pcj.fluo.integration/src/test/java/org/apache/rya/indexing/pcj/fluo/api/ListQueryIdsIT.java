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
package org.apache.rya.indexing.pcj.fluo.api;

import static org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns.QUERY_ID;
import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.fluo.api.client.Transaction;
import org.apache.rya.pcj.fluo.test.base.RyaExportITBase;
import org.junit.Test;

import com.beust.jcommander.internal.Lists;

/**
 * Integration tests the methods of {@link ListQueryIds}.
 */
public class ListQueryIdsIT extends RyaExportITBase {

    /**
     * This test ensures that when there are PCJ tables in Accumulo as well as
     * the Fluo table's export destinations column, the command for fetching the
     * list of queries only includes queries that appear in both places.
     */
    @Test
    public void getQueryIds() throws AccumuloException, AccumuloSecurityException, TableExistsException {
        try(FluoClient fluoClient = FluoFactory.newClient(super.getFluoConfiguration())) {
            // Store a few SPARQL/Query ID pairs in the Fluo table.
            try(Transaction tx = fluoClient.newTransaction()) {
                tx.set("SPARQL_3", QUERY_ID, "ID_3");
                tx.set("SPARQL_1", QUERY_ID, "ID_1");
                tx.set("SPARQL_4", QUERY_ID, "ID_4");
                tx.set("SPARQL_2", QUERY_ID, "ID_2");
                tx.commit();
            }

            // Ensure the correct list of Query IDs is retured.
            final List<String> expected = Lists.newArrayList("ID_1", "ID_2", "ID_3", "ID_4");
            final List<String> queryIds = new ListQueryIds().listQueryIds(fluoClient);
            assertEquals(expected, queryIds);
        }
    }
}