package org.apache.rya.accumulo.spark;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.api.domain.StatementMetadata;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.helpers.StatementPatternCollector;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

public class AccumuloTriplePatternDataframeGeneratorTest {

    private PasswordToken password = new PasswordToken("");
    private Instance instance;
    private AccumuloRyaDAO dao;
    private SparkSession spark;
    private AccumuloRdfConfiguration conf;

    private AccumuloRdfConfiguration getConf() {
        AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();
        conf.set(ConfigUtils.CLOUDBASE_AUTHS, "");
        conf.set(ConfigUtils.CLOUDBASE_USER, "root");
        conf.set(ConfigUtils.CLOUDBASE_INSTANCE, "instance");
        conf.set(ConfigUtils.CLOUDBASE_PASSWORD, "");
        conf.setTablePrefix("rya_");
        conf.setBoolean(ConfigUtils.USE_MOCK_INSTANCE, true);
        return conf;
    }

    @Before
    public void init() throws Exception {
        instance = new MockInstance("instance");
        Connector connector = instance.getConnector("root", password);
        conf = getConf();
        dao = new AccumuloRyaDAO();
        dao.setConf(conf);
        dao.setConnector(connector);
        dao.init();
        spark = SparkSession.builder().appName("test").master("local[4]").getOrCreate();
    }

    @After
    public void after() throws Exception {
        dao.dropAndDestroy();
        spark.close();
    }

    @Test
    public void testPoSpToDataset() throws Exception {
        RyaStatement input1 = RyaStatement.builder().setSubject(new RyaURI("http://Bob")).setPredicate(new RyaURI("http://worksAt"))
                .setObject(new RyaURI("http://www.google.com")).setContext(new RyaURI("http://context_1")).setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata()).build();

        RyaStatement input2 = RyaStatement.builder().setSubject(new RyaURI("http://Joe")).setPredicate(new RyaURI("http://worksAt"))
                .setObject(new RyaURI("http://www.yahoo.com")).setContext(new RyaURI("http://context_1")).setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata()).build();

        RyaStatement input3 = RyaStatement.builder().setSubject(new RyaURI("http://Joe")).setPredicate(new RyaURI("http://livesIn"))
                .setObject(new RyaURI("http://Virginia")).setContext(new RyaURI("http://context_2")).setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata()).build();

        dao.add(input1);
        dao.add(input2);
        dao.add(input3);

        String sparql = "select ?x {?x <http://worksAt> ?y. }";
        List<StatementPattern> spList = getSpFromSparql(sparql);
        AccumuloTriplePatternDataframeGenerator gen = new AccumuloTriplePatternDataframeGenerator(spark, conf);
        Dataset<Row> dataset = gen.getTriplePatternDataFrame(spList.get(0));

        List<Row> rows = dataset.collectAsList();
        Assert.assertEquals(2, rows.size());
        for (Row row : rows) {
            Assert.assertEquals(true, validateRow(row, spList.get(0)));
        }

        dao.delete(Arrays.asList(input1, input2, input3).iterator(), getConf());
    }
    
    @Test
    public void testSpoSpToDataset() throws Exception {
        RyaStatement input1 = RyaStatement.builder().setSubject(new RyaURI("http://Bob")).setPredicate(new RyaURI("http://worksAt"))
                .setObject(new RyaURI("http://www.google.com")).setContext(new RyaURI("http://context_1")).setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata()).build();

        RyaStatement input2 = RyaStatement.builder().setSubject(new RyaURI("http://Joe")).setPredicate(new RyaURI("http://worksAt"))
                .setObject(new RyaURI("http://www.yahoo.com")).setContext(new RyaURI("http://context_1")).setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata()).build();

        RyaStatement input3 = RyaStatement.builder().setSubject(new RyaURI("http://Joe")).setPredicate(new RyaURI("http://livesIn"))
                .setObject(new RyaURI("http://Virginia")).setContext(new RyaURI("http://context_2")).setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata()).build();

        dao.add(input1);
        dao.add(input2);
        dao.add(input3);

        String sparql = "select ?x ?y {<http://Joe> ?x ?y. }";
        List<StatementPattern> spList = getSpFromSparql(sparql);
        AccumuloTriplePatternDataframeGenerator gen = new AccumuloTriplePatternDataframeGenerator(spark, conf);
        Dataset<Row> dataset = gen.getTriplePatternDataFrame(spList.get(0));

        List<Row> rows = dataset.collectAsList();
        Assert.assertEquals(2, rows.size());
        for (Row row : rows) {
            Assert.assertEquals(true, validateRow(row, spList.get(0)));
        }

        dao.delete(Arrays.asList(input1, input2, input3).iterator(), getConf());

    }
    
    @Test
    public void testOspSpToDataset() throws Exception {
        RyaStatement input1 = RyaStatement.builder().setSubject(new RyaURI("http://Bob")).setPredicate(new RyaURI("http://worksAt"))
                .setObject(new RyaURI("http://www.google.com")).setContext(new RyaURI("http://context_1")).setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata()).build();

        RyaStatement input2 = RyaStatement.builder().setSubject(new RyaURI("http://Joe")).setPredicate(new RyaURI("http://worksAt"))
                .setObject(new RyaURI("http://www.yahoo.com")).setContext(new RyaURI("http://context_1")).setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata()).build();

        RyaStatement input3 = RyaStatement.builder().setSubject(new RyaURI("http://Joe")).setPredicate(new RyaURI("http://livesIn"))
                .setObject(new RyaURI("http://Virginia")).setContext(new RyaURI("http://context_2")).setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata()).build();

        dao.add(input1);
        dao.add(input2);
        dao.add(input3);

        String sparql = "select ?x ?y {?x ?y <http://www.google.com> . }";
        List<StatementPattern> spList = getSpFromSparql(sparql);
        AccumuloTriplePatternDataframeGenerator gen = new AccumuloTriplePatternDataframeGenerator(spark, conf);
        Dataset<Row> dataset = gen.getTriplePatternDataFrame(spList.get(0));

        List<Row> rows = dataset.collectAsList();
        Assert.assertEquals(1, rows.size());
        for (Row row : rows) {
            Assert.assertEquals(true, validateRow(row, spList.get(0)));
        }

        dao.delete(Arrays.asList(input1, input2, input3).iterator(), getConf());

    }
    
    
    @Test
    public void testOspSpToDatasetWithContext() throws Exception {
        RyaStatement input1 = RyaStatement.builder().setSubject(new RyaURI("http://Bob")).setPredicate(new RyaURI("http://worksAt"))
                .setObject(new RyaURI("http://www.google.com")).setContext(new RyaURI("http://context_1")).setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata()).build();

        RyaStatement input2 = RyaStatement.builder().setSubject(new RyaURI("http://Joe")).setPredicate(new RyaURI("http://worksAt"))
                .setObject(new RyaURI("http://www.yahoo.com")).setContext(new RyaURI("http://context_1")).setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata()).build();

        RyaStatement input3 = RyaStatement.builder().setSubject(new RyaURI("http://Joe")).setPredicate(new RyaURI("http://livesIn"))
                .setObject(new RyaURI("http://Virginia")).setContext(new RyaURI("http://context_2")).setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata()).build();

        dao.add(input1);
        dao.add(input2);
        dao.add(input3);

        String sparql = "select ?x ?y ?c {graph ?c {?x ?y <http://www.google.com> . }}";
        List<StatementPattern> spList = getSpFromSparql(sparql);
        AccumuloTriplePatternDataframeGenerator gen = new AccumuloTriplePatternDataframeGenerator(spark, conf);
        Dataset<Row> dataset = gen.getTriplePatternDataFrame(spList.get(0));

        List<Row> rows = dataset.collectAsList();
        Assert.assertEquals(1, rows.size());
        for (Row row : rows) {
            Assert.assertEquals(true, validateRow(row, spList.get(0)));
        }

        dao.delete(Arrays.asList(input1, input2, input3).iterator(), getConf());

    }
    
    @Test
    public void testSpoSpToDatasetWithConstantContext() throws Exception {
        RyaStatement input1 = RyaStatement.builder().setSubject(new RyaURI("http://Bob")).setPredicate(new RyaURI("http://worksAt"))
                .setObject(new RyaURI("http://www.google.com")).setContext(new RyaURI("http://context_1")).setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata()).build();

        RyaStatement input2 = RyaStatement.builder().setSubject(new RyaURI("http://Joe")).setPredicate(new RyaURI("http://worksAt"))
                .setObject(new RyaURI("http://www.yahoo.com")).setContext(new RyaURI("http://context_1")).setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata()).build();

        RyaStatement input3 = RyaStatement.builder().setSubject(new RyaURI("http://Joe")).setPredicate(new RyaURI("http://livesIn"))
                .setObject(new RyaURI("http://Virginia")).setContext(new RyaURI("http://context_2")).setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata()).build();

        dao.add(input1);
        dao.add(input2);
        dao.add(input3);

        String sparql = "select ?x ?y {graph <http://context_1> {<http://Joe> ?x ?y . }}";
        List<StatementPattern> spList = getSpFromSparql(sparql);
        AccumuloTriplePatternDataframeGenerator gen = new AccumuloTriplePatternDataframeGenerator(spark, conf);
        Dataset<Row> dataset = gen.getTriplePatternDataFrame(spList.get(0));

        List<Row> rows = dataset.collectAsList();
        Assert.assertEquals(1, rows.size());
        for (Row row : rows) {
            Assert.assertEquals(true, validateRow(row, spList.get(0)));
        }

        dao.delete(Arrays.asList(input1, input2, input3).iterator(), getConf());

    }
    
    @Test
    public void testPoRyaStatementToDataset() throws Exception {
        RyaStatement input1 = RyaStatement.builder().setSubject(new RyaURI("http://Bob")).setPredicate(new RyaURI("http://worksAt"))
                .setObject(new RyaURI("http://www.google.com")).setContext(new RyaURI("http://context_1")).setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata()).build();

        RyaStatement input2 = RyaStatement.builder().setSubject(new RyaURI("http://Joe")).setPredicate(new RyaURI("http://worksAt"))
                .setObject(new RyaURI("http://www.yahoo.com")).setContext(new RyaURI("http://context_1")).setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata()).build();

        RyaStatement input3 = RyaStatement.builder().setSubject(new RyaURI("http://Joe")).setPredicate(new RyaURI("http://livesIn"))
                .setObject(new RyaURI("http://Virginia")).setContext(new RyaURI("http://context_2")).setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata()).build();

        dao.add(input1);
        dao.add(input2);
        dao.add(input3);

        AccumuloTriplePatternDataframeGenerator gen = new AccumuloTriplePatternDataframeGenerator(spark, conf);
        Dataset<Row> dataset = gen.getTriplePatternDataFrame(new RyaStatement(null, new RyaURI("http://worksAt"), null));

        List<Row> rows = dataset.collectAsList();
        Assert.assertEquals(2, rows.size());
        for (Row row : rows) {
            Assert.assertEquals(true, validateRow(row, null));
        }

        dao.delete(Arrays.asList(input1, input2, input3).iterator(), getConf());

    }
    
    
    
    @Test
    public void testSpRyaStatementToDataset() throws Exception {
        RyaStatement input1 = RyaStatement.builder().setSubject(new RyaURI("http://Bob")).setPredicate(new RyaURI("http://worksAt"))
                .setObject(new RyaURI("http://www.google.com")).setContext(new RyaURI("http://context_1")).setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata()).build();

        RyaStatement input2 = RyaStatement.builder().setSubject(new RyaURI("http://Joe")).setPredicate(new RyaURI("http://worksAt"))
                .setObject(new RyaURI("http://www.yahoo.com")).setContext(new RyaURI("http://context_1")).setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata()).build();

        RyaStatement input3 = RyaStatement.builder().setSubject(new RyaURI("http://Joe")).setPredicate(new RyaURI("http://livesIn"))
                .setObject(new RyaURI("http://Virginia")).setContext(new RyaURI("http://context_2")).setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata()).build();

        dao.add(input1);
        dao.add(input2);
        dao.add(input3);

        AccumuloTriplePatternDataframeGenerator gen = new AccumuloTriplePatternDataframeGenerator(spark, conf);
        Dataset<Row> dataset = gen.getTriplePatternDataFrame(new RyaStatement(new RyaURI("http://Joe"), null, null));

        List<Row> rows = dataset.collectAsList();
        Assert.assertEquals(2, rows.size());
        for (Row row : rows) {
            Assert.assertEquals(true, validateRow(row, null));
        }

        dao.delete(Arrays.asList(input1, input2, input3).iterator(), getConf());

    }

    private boolean validateRow(Row row, StatementPattern sp) {
        boolean valid = true;
        if (sp == null) {
            valid = row.size() == 4;
            List<String> schema = Arrays.asList(row.schema().fieldNames());
            valid = valid && schema.contains("subject");
            valid = valid && schema.contains("predicate");
            valid = valid && schema.contains("object");
            valid = valid && schema.contains("context");
        } else {
            List<Var> varList = sp.getVarList();
            List<String> vars = new ArrayList<>();
            for (Var var : varList) {
                if (!var.isConstant()) {
                    vars.add(var.getName());
                }
            }
            List<String> schema = Arrays.asList(row.schema().fieldNames());
            //context not included in varList if context == null
            if (sp.getContextVar() == null) {
                valid = row.size() - 1 == vars.size();
            } else {
                valid = row.size() == vars.size();
            }
            
            for (String var : vars) {
                valid = valid && schema.contains(var);
            }
        }
        return valid;
    }


    private List<StatementPattern> getSpFromSparql(String query) throws MalformedQueryException {
        SPARQLParser parser = new SPARQLParser();
        ParsedQuery pq = parser.parseQuery(query, null);
        return StatementPatternCollector.process(pq.getTupleExpr());
    }

}
