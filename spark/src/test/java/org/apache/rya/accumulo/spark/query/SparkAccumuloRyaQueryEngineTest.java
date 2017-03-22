package org.apache.rya.accumulo.spark.query;

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
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.api.domain.StatementMetadata;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.spark.query.SparkRyaQueryException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;

import info.aduna.iteration.CloseableIteration;

public class SparkAccumuloRyaQueryEngineTest {

    private PasswordToken password = new PasswordToken("");
    private Instance instance;
    private AccumuloRyaDAO dao;
    private SparkSession spark;
    private AccumuloRdfConfiguration conf;
    private SparkAccumuloRyaQueryEngine engine;

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
        engine = new SparkAccumuloRyaQueryEngine(spark, conf);
    }

    @After
    public void after() throws Exception {
        dao.dropAndDestroy();
        spark.close();
    }
    
    @Test
    public void testQueryEngine() throws RyaDAOException, SparkRyaQueryException, QueryEvaluationException {
        
        RyaStatement input1 = RyaStatement.builder().setSubject(new RyaURI("http://Bob")).setPredicate(new RyaURI("http://worksAt"))
                .setObject(new RyaURI("http://www.google.com")).setContext(new RyaURI("http://context_1")).setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata()).build();
        
        RyaStatement input2 = RyaStatement.builder().setSubject(new RyaURI("http://Bob")).setPredicate(new RyaURI("http://livesIn"))
                .setObject(new RyaURI("http://Washington")).setContext(new RyaURI("http://context_1")).setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata()).build();
        
        RyaStatement input3 = RyaStatement.builder().setSubject(new RyaURI("http://Bob")).setPredicate(new RyaURI("http://livesIn"))
                .setObject(new RyaURI("http://Maryland")).setContext(new RyaURI("http://context_2")).setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata()).build();

        RyaStatement input4 = RyaStatement.builder().setSubject(new RyaURI("http://Joe")).setPredicate(new RyaURI("http://worksAt"))
                .setObject(new RyaURI("http://www.yahoo.com")).setContext(new RyaURI("http://context_2")).setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata()).build();

        RyaStatement input5 = RyaStatement.builder().setSubject(new RyaURI("http://Joe")).setPredicate(new RyaURI("http://livesIn"))
                .setObject(new RyaURI("http://Virginia")).setContext(new RyaURI("http://context_2")).setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata()).build();
        
        RyaStatement input6 = RyaStatement.builder().setSubject(new RyaURI("http://Joe")).setPredicate(new RyaURI("http://worksAt"))
                .setObject(new RyaURI("http://Apple")).setContext(new RyaURI("http://context_3")).setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata()).build();

        dao.add(input1);
        dao.add(input2);
        dao.add(input3);
        dao.add(input4);
        dao.add(input5);
        dao.add(input6);
        
        CloseableIteration<BindingSet, QueryEvaluationException> results = engine.query("select ?x ?y ?z { ?x <http://worksAt> ?y; <http://livesIn> ?z.}");
        int count = 0;
        while(results.hasNext()) {
            Assert.assertEquals(true, validateBindingSet(results.next(), Arrays.asList("x","y","z")));
            count++;
        }
        Assert.assertEquals(2, count);
        
        dao.delete(Arrays.asList(input1, input2, input3).iterator(), conf);
        
    }
    
    @Test
    public void testQueryEngineNamedGraph() throws RyaDAOException, SparkRyaQueryException, QueryEvaluationException {
        
        RyaStatement input1 = RyaStatement.builder().setSubject(new RyaURI("http://Bob")).setPredicate(new RyaURI("http://worksAt"))
                .setObject(new RyaURI("http://www.google.com")).setContext(new RyaURI("http://context_1")).setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata()).build();
        
        RyaStatement input2 = RyaStatement.builder().setSubject(new RyaURI("http://Bob")).setPredicate(new RyaURI("http://livesIn"))
                .setObject(new RyaURI("http://Washington")).setContext(new RyaURI("http://context_1")).setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata()).build();
        
        RyaStatement input3 = RyaStatement.builder().setSubject(new RyaURI("http://Bob")).setPredicate(new RyaURI("http://livesIn"))
                .setObject(new RyaURI("http://Maryland")).setContext(new RyaURI("http://context_2")).setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata()).build();

        RyaStatement input4 = RyaStatement.builder().setSubject(new RyaURI("http://Joe")).setPredicate(new RyaURI("http://worksAt"))
                .setObject(new RyaURI("http://www.yahoo.com")).setContext(new RyaURI("http://context_2")).setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata()).build();

        RyaStatement input5 = RyaStatement.builder().setSubject(new RyaURI("http://Joe")).setPredicate(new RyaURI("http://livesIn"))
                .setObject(new RyaURI("http://Virginia")).setContext(new RyaURI("http://context_2")).setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata()).build();
        
        RyaStatement input6 = RyaStatement.builder().setSubject(new RyaURI("http://Joe")).setPredicate(new RyaURI("http://worksAt"))
                .setObject(new RyaURI("http://Apple")).setContext(new RyaURI("http://context_3")).setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata()).build();

        dao.add(input1);
        dao.add(input2);
        dao.add(input3);
        dao.add(input4);
        dao.add(input5);
        dao.add(input6);
        
        CloseableIteration<BindingSet, QueryEvaluationException> results = engine.query("select ?x ?y ?z ?c { graph ?c { ?x <http://worksAt> ?y; <http://livesIn> ?z.}}");
        int count = 0;
        while(results.hasNext()) {
            Assert.assertEquals(true, validateBindingSet(results.next(), Arrays.asList("x","y","z","c")));
            count++;
        }
        Assert.assertEquals(2, count);
        
        dao.delete(Arrays.asList(input1, input2, input3).iterator(), conf);
        
    }
    
    

    @Test
    public void testQueryEngineNamedConstantGraph() throws RyaDAOException, SparkRyaQueryException, QueryEvaluationException {
        
        RyaStatement input1 = RyaStatement.builder().setSubject(new RyaURI("http://Bob")).setPredicate(new RyaURI("http://worksAt"))
                .setObject(new RyaURI("http://www.google.com")).setContext(new RyaURI("http://context_1")).setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata()).build();
        
        RyaStatement input2 = RyaStatement.builder().setSubject(new RyaURI("http://Bob")).setPredicate(new RyaURI("http://livesIn"))
                .setObject(new RyaURI("http://Washington")).setContext(new RyaURI("http://context_1")).setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata()).build();
        
        RyaStatement input3 = RyaStatement.builder().setSubject(new RyaURI("http://Bob")).setPredicate(new RyaURI("http://livesIn"))
                .setObject(new RyaURI("http://Maryland")).setContext(new RyaURI("http://context_2")).setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata()).build();

        RyaStatement input4 = RyaStatement.builder().setSubject(new RyaURI("http://Joe")).setPredicate(new RyaURI("http://worksAt"))
                .setObject(new RyaURI("http://www.yahoo.com")).setContext(new RyaURI("http://context_2")).setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata()).build();

        RyaStatement input5 = RyaStatement.builder().setSubject(new RyaURI("http://Joe")).setPredicate(new RyaURI("http://livesIn"))
                .setObject(new RyaURI("http://Virginia")).setContext(new RyaURI("http://context_2")).setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata()).build();
        
        RyaStatement input6 = RyaStatement.builder().setSubject(new RyaURI("http://Joe")).setPredicate(new RyaURI("http://worksAt"))
                .setObject(new RyaURI("http://Apple")).setContext(new RyaURI("http://context_3")).setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata()).build();

        dao.add(input1);
        dao.add(input2);
        dao.add(input3);
        dao.add(input4);
        dao.add(input5);
        dao.add(input6);
        
        CloseableIteration<BindingSet, QueryEvaluationException> results = engine.query("select ?x ?y ?z { graph <http://context_1> { ?x <http://worksAt> ?y; <http://livesIn> ?z.}}");
        int count = 0;
        while(results.hasNext()) {
            Assert.assertEquals(true, validateBindingSet(results.next(), Arrays.asList("x","y","z")));
            count++;
        }
        Assert.assertEquals(1, count);
        dao.delete(Arrays.asList(input1, input2, input3).iterator(), conf);
        
    }
    
    @Test
    public void testQueryEngineTwoJoins() throws RyaDAOException, SparkRyaQueryException, QueryEvaluationException {
        
        RyaStatement input1 = RyaStatement.builder().setSubject(new RyaURI("http://Bob")).setPredicate(new RyaURI("http://worksAt"))
                .setObject(new RyaURI("http://www.google.com")).setContext(new RyaURI("http://context_1")).setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata()).build();
        
        RyaStatement input2 = RyaStatement.builder().setSubject(new RyaURI("http://Bob")).setPredicate(new RyaURI("http://livesIn"))
                .setObject(new RyaURI("http://Washington")).setContext(new RyaURI("http://context_1")).setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata()).build();
        
        RyaStatement input3 = RyaStatement.builder().setSubject(new RyaURI("http://www.google.com")).setPredicate(new RyaURI("http://numberOfEmployees"))
                .setObject(new RyaType(XMLSchema.INT, "5000")).setContext(new RyaURI("http://context_1")).setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata()).build();
        
        RyaStatement input4 = RyaStatement.builder().setSubject(new RyaURI("http://www.yahoo.com")).setPredicate(new RyaURI("http://numberOfEmployees"))
                .setObject(new RyaType(XMLSchema.INT, "2000")).setContext(new RyaURI("http://context_1")).setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata()).build();

        dao.add(input1);
        dao.add(input2);
        dao.add(input3);
        dao.add(input4);
        
        CloseableIteration<BindingSet, QueryEvaluationException> results = engine.query("select ?a ?b ?c { ?a <http://worksAt> ?b; <http://livesIn> <http://Washington> . ?b <http://numberOfEmployees> ?c }");
        
        int count = 0;
        while(results.hasNext()) {
            Assert.assertEquals(true, validateBindingSet(results.next(), Arrays.asList("a", "b", "c")));
            count++;
        }
        Assert.assertEquals(1, count);
        
        dao.delete(Arrays.asList(input1, input2, input3, input4).iterator(), conf);
        
    }
    
    
    
    @Test
    public void testQueryEngineThreeJoins() throws RyaDAOException, SparkRyaQueryException, QueryEvaluationException {
        
        RyaStatement input1 = RyaStatement.builder().setSubject(new RyaURI("http://Bob")).setPredicate(new RyaURI("http://worksAt"))
                .setObject(new RyaURI("http://www.google.com")).setContext(new RyaURI("http://context_1")).setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata()).build();
        
        RyaStatement input2 = RyaStatement.builder().setSubject(new RyaURI("http://Bob")).setPredicate(new RyaURI("http://livesIn"))
                .setObject(new RyaURI("http://Washington")).setContext(new RyaURI("http://context_1")).setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata()).build();
        
        RyaStatement input3 = RyaStatement.builder().setSubject(new RyaURI("http://Bob")).setPredicate(new RyaURI("http://talksTo"))
                .setObject(new RyaURI("http://Doug")).setContext(new RyaURI("http://context_1")).setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata()).build();

        RyaStatement input4 = RyaStatement.builder().setSubject(new RyaURI("http://Bob")).setPredicate(new RyaURI("http://commutesBy"))
                .setObject(new RyaURI("http://Bike")).setContext(new RyaURI("http://context_1")).setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata()).build();


        dao.add(input1);
        dao.add(input2);
        dao.add(input3);
        dao.add(input4);
        
        CloseableIteration<BindingSet, QueryEvaluationException> results = engine.query("select ?a ?b ?c ?d ?e { ?a <http://worksAt> ?b; <http://livesIn> ?c; <http://talksTo> ?d; <http://commutesBy> ?e.}");
        
        int count = 0;
        while(results.hasNext()) {
            Assert.assertEquals(true, validateBindingSet(results.next(), Arrays.asList("a", "b", "c", "d", "e")));
            count++;
        }
        Assert.assertEquals(1, count);
        
        dao.delete(Arrays.asList(input1, input2, input3, input4).iterator(), conf);
        
    }
    
    @Test
    public void testQueryEngineNullContext() throws RyaDAOException, SparkRyaQueryException, QueryEvaluationException {
        
        RyaStatement input1 = RyaStatement.builder().setSubject(new RyaURI("http://Bob")).setPredicate(new RyaURI("http://worksAt"))
                .setObject(new RyaURI("http://www.google.com")).setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata()).build();
        
        RyaStatement input2 = RyaStatement.builder().setSubject(new RyaURI("http://Bob")).setPredicate(new RyaURI("http://livesIn"))
                .setObject(new RyaURI("http://Washington")).setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata()).build();
        
        RyaStatement input3 = RyaStatement.builder().setSubject(new RyaURI("http://Bob")).setPredicate(new RyaURI("http://livesIn"))
                .setObject(new RyaURI("http://Maryland")).setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata()).build();

        RyaStatement input4 = RyaStatement.builder().setSubject(new RyaURI("http://Joe")).setPredicate(new RyaURI("http://worksAt"))
                .setObject(new RyaURI("http://www.yahoo.com")).setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata()).build();

        RyaStatement input5 = RyaStatement.builder().setSubject(new RyaURI("http://Joe")).setPredicate(new RyaURI("http://livesIn"))
                .setObject(new RyaURI("http://Virginia")).setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata()).build();
        
        RyaStatement input6 = RyaStatement.builder().setSubject(new RyaURI("http://Joe")).setPredicate(new RyaURI("http://worksAt"))
                .setObject(new RyaURI("http://Apple")).setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata()).build();

        dao.add(input1);
        dao.add(input2);
        dao.add(input3);
        dao.add(input4);
        dao.add(input5);
        dao.add(input6);
        
        CloseableIteration<BindingSet, QueryEvaluationException> results = engine.query("select ?x ?y ?z { ?x <http://worksAt> ?y; <http://livesIn> ?z.}");
        int count = 0;
        while(results.hasNext()) {
            Assert.assertEquals(true, validateBindingSet(results.next(), Arrays.asList("x","y","z")));
            count++;
        }
        Assert.assertEquals(4, count);
        dao.delete(Arrays.asList(input1, input2, input3, input4, input5, input6).iterator(), conf);
        
    }
    
    
    private boolean validateBindingSet(BindingSet bs, List<String> vars) {
        boolean valid = true;
        valid = bs.size() == vars.size();

        for (String var : vars) {
            valid = valid && bs.hasBinding(var);
        }
        return valid;
    }
    
    
}
