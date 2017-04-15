package org.apache.rya.indexing.pcj.fluo.integration;
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
import java.util.Set;

import org.apache.rya.indexing.pcj.fluo.ITBase;
import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;
import org.openrdf.repository.RepositoryException;

import com.google.common.collect.Sets;

public class ConstructQueryIT extends ITBase {

    @Test
    public void testConstruct() throws MalformedQueryException, RepositoryException, QueryEvaluationException {

        String query = "construct { ?x <urn:grandMotherOf> ?z. ?y <urn:daughterOf> ?x} where {?x <urn:motherOf> ?y. ?y <urn:motherOf> ?z. }";

        SPARQLParser parser = new SPARQLParser();
        ParsedQuery pq = parser.parseQuery(query, null);
        System.out.println("Parsed query: " + pq.getTupleExpr());

        // Triples that are loaded into Rya before the PCJ is created.
        final Set<Statement> historicTriples = Sets.newHashSet(makeStatement("urn:Alice", "urn:motherOf", "urn:Eve"),
                makeStatement("urn:Eve", "urn:motherOf", "urn:Joan"));

        // Load the historic data into Rya.
        for (final Statement triple : historicTriples) {
            ryaConn.add(triple);
        }
        
        GraphQuery graphResult = ryaConn.prepareGraphQuery(QueryLanguage.SPARQL, query);
        GraphQueryResult result = graphResult.evaluate();
        while (result.hasNext()) {
            System.out.println(result.next());
        }

    }

}
