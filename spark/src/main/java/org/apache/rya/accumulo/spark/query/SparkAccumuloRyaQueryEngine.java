package org.apache.rya.accumulo.spark.query;

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
