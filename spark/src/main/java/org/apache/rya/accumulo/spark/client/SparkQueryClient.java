package org.apache.rya.accumulo.spark.client;

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
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.spark.query.SparkAccumuloRyaQueryEngine;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.rdftriplestore.RdfCloudTripleStore;
import org.apache.rya.rdftriplestore.inference.InferenceEngine;
import org.apache.rya.sail.config.RyaSailFactory;
import org.apache.spark.sql.SparkSession;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.RepositoryResult;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.rio.RDFFormat;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailException;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import info.aduna.iteration.CloseableIteration;
import jersey.repackaged.com.google.common.base.Preconditions;

public class SparkQueryClient {

    private static final Logger log = Logger.getLogger(SparkQueryClient.class);

    private static AccumuloRdfConfiguration getConf(Properties props) {
        AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();
        conf.set(ConfigUtils.CLOUDBASE_AUTHS, props.getProperty("accumulo.auths"));
        conf.set(ConfigUtils.CLOUDBASE_USER, props.getProperty("accumulo.user"));
        conf.set(ConfigUtils.CLOUDBASE_INSTANCE, props.getProperty("accumulo.instance"));
        conf.set(ConfigUtils.CLOUDBASE_PASSWORD, props.getProperty("accumulo.password"));
        conf.set(ConfigUtils.CLOUDBASE_ZOOKEEPERS, props.getProperty("accumulo.zoo"));
        conf.setTablePrefix(props.getProperty("rya.prefix"));
        conf.set("rya.useOldEngine", props.getProperty("rya.useOldEngine"));
        conf.set(ConfigUtils.USE_MOCK_INSTANCE, props.getProperty("accumulo.mock"));
//        conf.setInfer(true);
        conf.setDisplayQueryPlan(true);
        return conf;
    }

    @Parameter(names = "-props")
    private String propPath;
    @Parameter(names = "-queries")
    private String queryPath;
    @Parameter(names = "-report")
    private String reportPath;
    @Parameter(names = "-iter")
    private int iter;
    @Parameter(names = "-triples")
    private String triplesPath;
    @Parameter(names = "-ont")
    private String ontologyPath;

    public static void main(String[] args) {

        SparkQueryClient client = new SparkQueryClient();
        new JCommander(client, args);
        Preconditions.checkNotNull(client.propPath);
        Preconditions.checkNotNull(client.queryPath);
        Preconditions.checkNotNull(client.reportPath);
        Preconditions.checkNotNull(client.iter);

        Properties props = new Properties();
        Sail sail = null;
        SailRepository repo = null;
        SailRepositoryConnection conn = null;

        try {
            props.load(new FileInputStream(client.propPath));
            AccumuloRdfConfiguration conf = getConf(props);
            boolean useOldEngine = conf.get("rya.useOldEngine").equalsIgnoreCase("true");
            if (useOldEngine) {
                log.info("Creating RyaSail Query Engine...");
                sail = createRya(conf);
                repo = new SailRepository(sail);
                conn = repo.getConnection();
            }

            if (client.triplesPath != null) {
                if (!useOldEngine) {
                    sail = createRya(conf);
                    repo = new SailRepository(sail);
                    conn = repo.getConnection();
                }
                addData(conn, client.triplesPath);
            }
            
            InferenceEngine infer = null;
            if(client.ontologyPath != null && sail != null) {
                addData(conn, client.ontologyPath);
                infer = ((RdfCloudTripleStore) sail).getInferenceEngine(); 
                if(infer != null) {
                    infer.refreshGraph();
                }
            }

            SparkSession spark = SparkSession.builder().appName(SparkQueryClient.class.getName()).master(props.getProperty("spark.master"))
                    .getOrCreate();
            SparkAccumuloRyaQueryEngine engine = new SparkAccumuloRyaQueryEngine(spark, conf);

            Files.deleteIfExists(Paths.get(client.reportPath));
            BufferedWriter writer = new BufferedWriter(new FileWriter(new File(client.reportPath)));

            for (Map.Entry<String, String> query : getQueries(new File(client.queryPath)).entrySet()) {
                client.evaluateSparkRyaQueryAndGenerateReportEntry(query.getKey(), query.getValue(), engine, client.iter, writer);
                if (useOldEngine) {
                    client.evaluateRyaQueryAndGenerateReportEntry(query.getKey(), query.getValue(), conn, client.iter, writer);
                }
            }

            writer.close();
            if(infer != null) {
                infer.destroy();
            }
            shutdown(sail, repo, conn);
            spark.close();
        } catch (Exception e) {
            log.info("Exiting the program due to an Exception");
            e.printStackTrace(System.out);
            System.exit(0);
        }
    }

    public static BufferedWriter getHDFSWriter(FileSystem hdfs, String outputPath) throws Exception {
        Path file = new Path(outputPath);
        if (hdfs.exists(file)) {
            hdfs.delete(file, true);
        }
        OutputStream os = hdfs.create(file);
        return new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
    }

    private static void shutdown(Sail sail, SailRepository repo, SailRepositoryConnection conn) throws RepositoryException, SailException {
        log.info("Shutting Down Local Rya Resources");
        if (conn != null) {
            conn.close();
        }
        if (repo != null) {
            repo.shutDown();
        }
        if (sail != null) {
            sail.shutDown();
        }
    }

    private void evaluateSparkRyaQueryAndGenerateReportEntry(String queryName, String query, SparkAccumuloRyaQueryEngine engine,
            int totalIterations, BufferedWriter writer) throws Exception {
        double[] first = new double[totalIterations];
        double[] firstTen = new double[totalIterations];
        double[] all = new double[totalIterations];
        int count = 0;

        log.info("( " + this.getClass().getName() + ".169 ) Issuing " + queryName + " with " + totalIterations + " iterations.");
        log.info("QueryString: " + query);
        for (int i = 0; i < totalIterations; i++) {
            count = evaluateSparkRyaIteration(query, engine, i, first, firstTen, all);
        }

        generateReportEntry("SPARK-RYA", queryName, totalIterations, count, getAverage(first), getAverage(firstTen), getAverage(all),
                writer);

    }

    private void evaluateRyaQueryAndGenerateReportEntry(String queryName, String query, SailRepositoryConnection engine,
            int totalIterations, BufferedWriter writer) throws Exception {
        double[] first = new double[totalIterations];
        double[] firstTen = new double[totalIterations];
        double[] all = new double[totalIterations];
        int count = 0;

        log.info("( " + this.getClass().getName() + ".186 ) Issuing " + queryName + " with " + totalIterations + " iterations.");
        log.info("QueryString: " + query);
        for (int i = 0; i < totalIterations; i++) {
            count = evaluateRyaIteration(query, engine, i, first, firstTen, all);
        }

        generateReportEntry("RYA", queryName, totalIterations, count, getAverage(first), getAverage(firstTen), getAverage(all), writer);

    }

    private void generateReportEntry(String queryEngine, String queryName, int totalIterations, int count, double aveFirst,
            double aveFirstTen, double aveAll, BufferedWriter writer) throws IOException {
        log.info("Generating performance report.");
        writer.write("*******************************************************************");
        writer.newLine();
        writer.write("Query Engine: " + queryEngine);
        writer.newLine();
        writer.write("Query: " + queryName);
        writer.newLine();
        writer.write("*******************************************************************");
        writer.newLine();
        writer.newLine();
        writer.write("Total Number of Iterations: " + totalIterations);
        writer.newLine();
        writer.write("Total Number of Results: " + count);
        writer.newLine();
        writer.write("Average Time to Return First Result: " + aveFirst);
        writer.newLine();
        writer.write("Average Time to Return First Ten Results: " + aveFirstTen);
        writer.newLine();
        writer.write("Average Time to Return All Results: " + aveAll);
        writer.newLine();
        writer.newLine();
        writer.newLine();
    }

    public double getAverage(double[] array) {
        double sum = 0;
        for (double d : array) {
            sum += d;
        }
        return sum / array.length;
    }

    private int evaluateSparkRyaIteration(String query, SparkAccumuloRyaQueryEngine engine, int iter, double[] first, double[] firstTen,
            double[] all) throws Exception {
        long start = System.currentTimeMillis();
        log.info("Issuing SPARK-RYA Query.");
        CloseableIteration<BindingSet, QueryEvaluationException> results = engine.query(query);
        int count = 0;
        double totalTime = 0;
        while (results.hasNext()) {
            results.next();
            count++;
            if (count == 1) {
                totalTime = ((double) System.currentTimeMillis() - start) / 1000;
                first[iter] = totalTime;
            }
            if (count == 10) {
                totalTime = ((double) System.currentTimeMillis() - start) / 1000;
                firstTen[iter] = totalTime;
            }
        }
        totalTime = ((double) System.currentTimeMillis() - start) / 1000;
        if (count < 1) {
            first[iter] = totalTime;
        }
        if (count < 10) {
            firstTen[iter] = totalTime;
        }
        all[iter] = totalTime;
        log.info("Finishing SPARK-RYA Query.");
        return count;
    }

    private int evaluateRyaIteration(String query, SailRepositoryConnection engine, int iter, double[] first, double[] firstTen,
            double[] all) throws Exception {
        long start = System.currentTimeMillis();
        log.info("Issuing RYA Query.");
        TupleQueryResult results = engine.prepareTupleQuery(QueryLanguage.SPARQL, query, null).evaluate();
        int count = 0;
        double totalTime = 0;
        while (results.hasNext()) {
            results.next();
            count++;
            if (count == 1) {
                totalTime = ((double) System.currentTimeMillis() - start) / 1000;
                first[iter] = totalTime;
            }
            if (count == 10) {
                totalTime = ((double) System.currentTimeMillis() - start) / 1000;
                firstTen[iter] = totalTime;
            }
        }
        totalTime = ((double) System.currentTimeMillis() - start) / 1000;
        if (count < 1) {
            first[iter] = totalTime;
        }
        if (count < 10) {
            firstTen[iter] = totalTime;
        }
        all[iter] = totalTime;
        log.info("Finishing RYA Query.");
        return count;
    }

    private static Map<String, String> getQueries(File queryFile) throws FileNotFoundException, IOException {
        Map<String, String> queries = new HashMap<>();
        log.info("Fetching queries");
        try (Scanner scanner = new Scanner(new FileReader(queryFile))) {
            StringBuilder builder = new StringBuilder();
            String next;
            String key = "";
            while (scanner.hasNext()) {
                next = scanner.nextLine().trim();
                if (next.startsWith("#")) {
                    continue;
                }
                // blank line
                if (next.equals("")) {
                    builder.append(next);
                    queries.put(key, builder.toString());
                    builder = new StringBuilder();
                    continue;
                }

                if (next.startsWith("Query")) {
                    key = next;
                    continue;
                }
                builder.append(next);
                builder.append(" ");
            }
            // add last query
            queries.put(key, builder.toString());

            return queries;
        }
    }
    
    private static Sail createRya(AccumuloRdfConfiguration conf) throws Exception {
        return RyaSailFactory.getInstance(conf);
    }

    private static void addData(SailRepositoryConnection conn, String triplesFile) throws Exception {
        log.info("Adding data from File: " + triplesFile);
        conn.add(new File(triplesFile), null, getFormat(triplesFile), new Resource[0]);
        log.info("Finished loading data.");
//        RepositoryResult<Statement> result = conn.getStatements(null, RDFS.SUBCLASSOF, null, false, new Resource[0]);
//        while(result.hasNext()) {
//            System.out.println(result.next());
//        }
        
    }
    
    private static RDFFormat getFormat(String triplesFile) {
        String[] nameAndExt = triplesFile.split("\\.");
        Preconditions.checkArgument(nameAndExt.length == 2);
        switch (nameAndExt[1].toLowerCase()) {
        case "xml":
            return RDFFormat.RDFXML;
        case "nt":
            return RDFFormat.NTRIPLES;
        case "ttl":
            return RDFFormat.TURTLE;
        case "trig":
            return RDFFormat.TRIG;
        default:
            throw new IllegalArgumentException("Invalid file type.");
        }
    }

}
