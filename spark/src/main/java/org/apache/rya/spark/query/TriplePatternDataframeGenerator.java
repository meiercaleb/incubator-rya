package org.apache.rya.spark.query;

import org.apache.rya.api.domain.RyaStatement;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.openrdf.query.algebra.StatementPattern;

public interface TriplePatternDataframeGenerator {

    public Dataset<Row> getTriplePatternDataFrame(RyaStatement pattern) throws SparkRyaQueryException;
    
    public Dataset<Row> getTriplePatternDataFrame(StatementPattern pattern) throws SparkRyaQueryException;
    
    public SparkSession getSparkSession();
}
