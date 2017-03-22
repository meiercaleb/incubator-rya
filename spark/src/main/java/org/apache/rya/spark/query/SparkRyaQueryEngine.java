package org.apache.rya.spark.query;

import org.apache.spark.sql.SparkSession;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;

import info.aduna.iteration.CloseableIteration;

public interface SparkRyaQueryEngine {

    public CloseableIteration<BindingSet, QueryEvaluationException> query(String sparql) throws SparkRyaQueryException;
    
    public SparkSession getSQLContext();
}
