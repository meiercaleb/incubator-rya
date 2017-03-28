package org.apache.rya.accumulo.spark.query;
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
import java.util.Map;

import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.spark.AccumuloTriplePatternDataframeGenerator;
import org.apache.rya.spark.query.SparkDatasetIterator;
import org.apache.rya.spark.query.SparkRyaQueryEngine;
import org.apache.rya.spark.query.SparkRyaQueryException;
import org.apache.rya.spark.query.SparqlQueryConversionUtils;
import org.apache.rya.spark.query.SparqlToSQLQueryConverter;
import org.apache.spark.sql.SparkSession;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

import info.aduna.iteration.CloseableIteration;

public class SparkAccumuloRyaQueryEngine implements SparkRyaQueryEngine {

    private static SPARQLParser parser = new SPARQLParser();
    private SparkSession spark;
    private AccumuloTriplePatternDataframeGenerator gen;
    private SparqlToSQLQueryConverter converter;

    public SparkAccumuloRyaQueryEngine(SparkSession spark, AccumuloRdfConfiguration conf) {
        this.spark = spark;
        this.gen = new AccumuloTriplePatternDataframeGenerator(spark, conf);
        this.converter = new SparqlToSQLQueryConverter();
    }

    @Override
    public SparkSession getSQLContext() {
        return spark;
    }

    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException> query(String sparql) throws SparkRyaQueryException {
        try {
            ParsedQuery pq = parser.parseQuery(sparql, null);
            TupleExpr te = pq.getTupleExpr();
            Map<StatementPattern, String> spTableNameMap = SparqlQueryConversionUtils.createAndRegisterSpTables(te, gen);
            return new SparkDatasetIterator(spark.sql(converter.convertSparqlToSQL(te, spTableNameMap)));
        } catch (Exception e) {
            throw new SparkRyaQueryException(e);
        }
    }
}
