package org.apache.rya.spark.query;

import org.apache.rya.spark.query.SparqlQueryConversionUtils;
import org.apache.rya.spark.query.SparqlToSQLQueryConverter;
import org.junit.Test;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

import org.junit.Assert;
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
public class SparqlToSQLQueryConverterTest {

    private static SPARQLParser parser = new SPARQLParser();
    
    @Test
    public void sparqlToSqlTest() throws MalformedQueryException {
        String sparql = "select ?t ?u ?v ?w ?x ?y ?z where { ?t <http://pred_1> ?u.?u <http://pred_2> ?v.?v <http://pred_3> ?w.?w <http://pred_4> ?x. ?x <http://pred_5> ?y. ?y <http://pred_6> ?z.}";
        String sql = "SELECT table1.t AS t, table2.u AS u, table2.v AS v, table4.w AS w, table5.x AS x, table6.y AS y, table6.z AS z FROM table1 JOIN table2 ON table1.context = table2.context AND table1.u = table2.u JOIN table3 ON table1.context = table3.context AND table2.v = table3.v JOIN table4 ON table1.context = table4.context AND table3.w = table4.w JOIN table5 ON table1.context = table5.context AND table4.x = table5.x JOIN table6 ON table1.context = table6.context AND table5.y = table6.y";

        SparqlToSQLQueryConverter converter = new SparqlToSQLQueryConverter();
        ParsedQuery pq = parser.parseQuery(sparql, null);
        TupleExpr te = pq.getTupleExpr();
        String actual = converter.convertSparqlToSQL(te, SparqlQueryConversionUtils.buildSpTableNameMap(te));
        System.out.println(actual);
        Assert.assertEquals(sql.replaceAll("\\s+", " "), actual.replaceAll("\\s+", " "));
    }
    
    @Test
    public void namedGraphSparqlToSQLTest() throws MalformedQueryException {
        String sparql = "select ?c ?t ?u ?v ?w ?x ?y ?z { graph ?c { ?t <http://pred_1> ?u.?u <http://pred_2> ?v.?v <http://pred_3> ?w.?w <http://pred_4> ?x. ?x <http://pred_5> ?y. ?y <http://pred_6> ?z.}}";
        String sql = "SELECT table6.c AS c, table1.t AS t, table2.u AS u, table2.v AS v, table4.w AS w, table5.x AS x, table6.y AS y, table6.z AS z FROM table1 JOIN table2 ON table1.c = table2.c AND table1.u = table2.u JOIN table3 ON table1.c = table3.c AND table2.v = table3.v JOIN table4 ON table1.c = table4.c AND table3.w = table4.w JOIN table5 ON table1.c = table5.c AND table4.x = table5.x JOIN table6 ON table1.c = table6.c AND table5.y = table6.y";

        SparqlToSQLQueryConverter converter = new SparqlToSQLQueryConverter();
        ParsedQuery pq = parser.parseQuery(sparql, null);
        TupleExpr te = pq.getTupleExpr();
        String actual = converter.convertSparqlToSQL(te, SparqlQueryConversionUtils.buildSpTableNameMap(te));
        System.out.println(actual);
        Assert.assertEquals(sql, actual);
    }
    
    @Test
    public void sparqlToSqlTestInputValidation() throws MalformedQueryException {
        String sparql = "select ?t ?u ?v { Filter(?t > 5) ?t <http://pred_1> ?u.?u <http://pred_2> ?v}";
        boolean valid = true;
        try {
            SparqlToSQLQueryConverter converter = new SparqlToSQLQueryConverter();
            ParsedQuery pq = parser.parseQuery(sparql, null);
            TupleExpr te = pq.getTupleExpr();
            converter.convertSparqlToSQL(te, SparqlQueryConversionUtils.buildSpTableNameMap(te));
        } catch (Exception e) {
            valid = false;
        }
        Assert.assertEquals(false, valid);
    }
    
    
    @Test
    public void sparqlToSqlTestConstantContext() throws MalformedQueryException {
        String sparql = "select ?t ?u ?v { graph <http://context_1> { ?t <http://pred_1> ?u.?u <http://pred_2> ?v}}";
        String sql = "SELECT table1.t AS t, table2.u AS u, table2.v AS v FROM table1 JOIN table2 ON table1.u = table2.u";

        SparqlToSQLQueryConverter converter = new SparqlToSQLQueryConverter();
        ParsedQuery pq = parser.parseQuery(sparql, null);
        TupleExpr te = pq.getTupleExpr();
        String actual = converter.convertSparqlToSQL(te, SparqlQueryConversionUtils.buildSpTableNameMap(te));
        System.out.println(actual);
        Assert.assertEquals(sql, actual);
    }

}
