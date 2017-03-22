package org.apache.rya.spark.query;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.helpers.StatementPatternCollector;

public class SparqlQueryConversionUtils {
    
    
    public static Map<StatementPattern, String> buildSpTableNameMap(TupleExpr te) throws MalformedQueryException {
        List<StatementPattern> patterns = StatementPatternCollector.process(te);
        AtomicInteger counter = new AtomicInteger(1);
        return patterns.stream().collect(Collectors.toMap(Function.identity(), x -> "table" + counter.getAndIncrement()));
    }
    
    public static void createAndRegisterSpTables(Map<StatementPattern, String> spTableNames, TriplePatternDataframeGenerator gen ) throws SparkRyaQueryException  {
        for (StatementPattern sp : spTableNames.keySet()) {
            Dataset<Row> dataset = gen.getTriplePatternDataFrame(sp);
            dataset.show();
            dataset.createOrReplaceTempView(spTableNames.get(sp));
            
        }
    }
    
    public static Map<StatementPattern, String> createAndRegisterSpTables(TupleExpr te, TriplePatternDataframeGenerator gen) throws MalformedQueryException, SparkRyaQueryException {
        Map<StatementPattern, String> spTableNameMap = buildSpTableNameMap(te);
        createAndRegisterSpTables(spTableNameMap, gen);
        return spTableNameMap;
    }
    
}
