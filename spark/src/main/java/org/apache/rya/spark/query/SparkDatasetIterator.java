package org.apache.rya.spark.query;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;

import info.aduna.iteration.ConvertingIteration;
import info.aduna.iteration.IteratorIteration;

public class SparkDatasetIterator extends ConvertingIteration<Row, BindingSet, QueryEvaluationException> {

    private String[] fieldNames;
    private ValueFactory vf = new ValueFactoryImpl();
    
    public SparkDatasetIterator(Dataset<Row> dataset) {
        super(new IteratorIteration<Row, QueryEvaluationException>(dataset.toLocalIterator()));
    }
    
    @Override
    public void remove() throws QueryEvaluationException {
        throw new UnsupportedOperationException();
    }

    @Override
    protected BindingSet convert(Row row) throws QueryEvaluationException {
        if(fieldNames == null) {
            fieldNames = row.schema().fieldNames();
        }
        
        QueryBindingSet bs = new QueryBindingSet();
        for(String name: fieldNames) {
            bs.addBinding(name, getValue(row.getAs(name)));
        }
        return bs;
    }
    
    private Value getValue(String stringValue) {
        String[] array = stringValue.split("\\^\\^");
        if(array.length == 1 || array[1].equals(XMLSchema.ANYURI.toString())) {
            return new URIImpl(array[0]);
        } 
        return vf.createLiteral(array[0], new URIImpl(array[1]));
    }

}
