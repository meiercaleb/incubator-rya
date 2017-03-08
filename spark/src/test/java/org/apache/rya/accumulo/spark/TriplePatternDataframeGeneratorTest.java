package org.apache.rya.accumulo.spark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.helpers.StatementPatternCollector;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

public class TriplePatternDataframeGeneratorTest implements Serializable {

    private static final long serialVersionUID = 1L;
    private PasswordToken password = new PasswordToken("");
    private Instance instance;
    private AccumuloRyaDAO dao;
    private JavaSparkContext sc;
    private SQLContext sql;

    private Configuration getConf() {
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
        AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();
        conf.setTablePrefix("rya_");
        dao = new AccumuloRyaDAO();
        dao.setConf(conf);
        dao.setConnector(connector);
        dao.init();

        SparkConf sparkConf = new SparkConf().setAppName("test").setMaster("local[4]");
        sc = new JavaSparkContext(sparkConf);
        sql = new SQLContext(sc);
    }

    @After
    public void after() throws Exception {
        dao.dropAndDestroy();
        sc.close();
    }

    // @Test
    public void testInputFormatSPO() throws Exception {

        addData(dao, 10000);

        TriplePatternDataframeGenerator gen = new TriplePatternDataframeGenerator(sql, getConf());
        DataFrame df1 = gen.getTriplePatternDataFrame(new RyaStatement(null, new RyaURI("http://pred_1"), null, null));
        DataFrame df2 = gen.getTriplePatternDataFrame(new RyaStatement(null, new RyaURI("http://pred_2"), null, null));
        DataFrame df3 = gen.getTriplePatternDataFrame(new RyaStatement(null, new RyaURI("http://pred_3"), null, null));

        df1.registerTempTable("table1");
        df2.registerTempTable("table2");
        df3.registerTempTable("table3");

        SQLContext sql = new SQLContext(sc);
        DataFrame queryResult = sql.sql("");
        queryResult.show();

    }

    @Test
    public void testInputSPO() throws Exception {

        addData(dao, 100000);

        TriplePatternDataframeGenerator gen = new TriplePatternDataframeGenerator(sql, getConf());
        List<StatementPattern> patterns = getSpFromSparql(
                "select ?t ?u ?v ?w ?x ?y ?z where { graph ?a {?t <http://pred_1> ?u.?u <http://pred_2> ?v.?v <http://pred_3> ?w.?w <http://pred_4> ?x. ?x <http://pred_5> ?y. ?y <http://pred_6> ?z.}}");
        List<DataFrame> dataFrames = new ArrayList<>();
        long start = System.currentTimeMillis();
        for (StatementPattern sp : patterns) {
            dataFrames.add(gen.getTriplePatternDataFrame(sp));
        }
        int i = 1;
        for (DataFrame frame : dataFrames) {
            frame.registerTempTable("table" + i);
            // System.out.println("Showing table"+i);
            // frame.show();
            i++;
        }

        DataFrame query = sql.sql(
                "SELECT table1.t, table1.u AS u1, table2.u AS u2, table2.v AS v1,table3.v AS v2, table3.w AS w1, table4.w AS w2, table4.x AS x1, table5.x AS x2, table5.y AS y1, table6.y AS y2, table6.z FROM table1 JOIN table2 ON table1.u = table2.u JOIN table3 ON table2.v = table3.v JOIN table4 ON table3.w = table4.w JOIN table5 ON table4.x = table5.x JOIN table6 ON table5.y = table6.y");
        System.out.println("Query Results: ");
        query.show();
        System.out.println("Total time: " + ((double) (System.currentTimeMillis() - start)) / 1000 + " seconds");

    }

    private void addData(AccumuloRyaDAO dao, int numTriples) throws RyaDAOException {
        List<String> subjList = Arrays.asList("http://subj_1", "http://subj_2", "http://subj_3", "http://subj_4",
                "http://subj_5");
        List<String> objList = Arrays.asList("http://obj_1", "http://obj_2", "http://obj_3", "http://obj_4",
                "http://obj_5");
        List<String> uriList = Arrays.asList("http://uri_1", "http://uri_2", "http://uri_3", "http://uri_4",
                "http://uri_5", "http://uri_6");
        List<String> predList = Arrays.asList("http://pred_1", "http://pred_2", "http://pred_3", "http://pred_4",
                "http://pred_5", "http://pred_6");
        List<String> contextList = Arrays.asList("http://context_1", "http://context_2");
        Random random = new Random();

        for (int i = 0; i < numTriples; i++) {
            String subj = uriList.get(random.nextInt(5));
            String pred = predList.get(random.nextInt(6));
            String obj = uriList.get(random.nextInt(5));
            String context = contextList.get(random.nextInt(2));
            String subj1 = subjList.get(random.nextInt(5));
            String obj1 = objList.get(random.nextInt(5));
            dao.add(new RyaStatement(new RyaURI(subj), new RyaURI(pred), new RyaURI(obj), new RyaURI(context)));
            dao.add(new RyaStatement(new RyaURI(subj1), new RyaURI(pred), new RyaURI(obj), new RyaURI(context)));
            dao.add(new RyaStatement(new RyaURI(subj), new RyaURI(pred), new RyaURI(obj1), new RyaURI(context)));
        }
    }

    private List<StatementPattern> getSpFromSparql(String query) throws MalformedQueryException {
        SPARQLParser parser = new SPARQLParser();
        ParsedQuery pq = parser.parseQuery(query, null);
        return StatementPatternCollector.process(pq.getTupleExpr());
    }

}
