package org.apache.rya.indexing.pcj.fluo.app;

import static org.junit.Assert.assertEquals;

import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSet;
import org.junit.Test;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.query.algebra.helpers.StatementPatternCollector;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

public class ConstructProjectionTest {

    private static final ValueFactory vf = new ValueFactoryImpl();
    
    @Test
    public void testConstructProjectionProjectSubj() throws MalformedQueryException, UnsupportedEncodingException {
        String query = "select ?x where { ?x <uri:talksTo> <uri:Bob> }";
        
        SPARQLParser parser = new SPARQLParser();
        ParsedQuery pq = parser.parseQuery(query, null);
        List<StatementPattern> patterns = StatementPatternCollector.process(pq.getTupleExpr());
        ConstructProjection projection = new ConstructProjection(patterns.get(0));
        
        QueryBindingSet bs = new QueryBindingSet();
        bs.addBinding("x", vf.createURI("uri:Joe"));
        VisibilityBindingSet vBs = new VisibilityBindingSet(bs, "FOUO");
        RyaStatement statement = projection.projectBindingSet(vBs);
        
        RyaStatement expected = new RyaStatement(new RyaURI("uri:Joe"), new RyaURI("uri:talksTo"), new RyaURI("uri:Bob"));
        expected.setColumnVisibility("FOUO".getBytes("UTF-8"));
        expected.setTimestamp(statement.getTimestamp());
        
        assertEquals(expected, statement);
    }
    
    @Test
    public void testConstructProjectionProjPred() throws MalformedQueryException {
        String query = "select ?p where { <uri:Joe> ?p <uri:Bob> }";
        
        SPARQLParser parser = new SPARQLParser();
        ParsedQuery pq = parser.parseQuery(query, null);
        List<StatementPattern> patterns = StatementPatternCollector.process(pq.getTupleExpr());
        ConstructProjection projection = new ConstructProjection(patterns.get(0));
        
        QueryBindingSet bs = new QueryBindingSet();
        bs.addBinding("p", vf.createURI("uri:worksWith"));
        VisibilityBindingSet vBs = new VisibilityBindingSet(bs);
        RyaStatement statement = projection.projectBindingSet(vBs);
        
        RyaStatement expected = new RyaStatement(new RyaURI("uri:Joe"), new RyaURI("uri:worksWith"), new RyaURI("uri:Bob"));
        expected.setTimestamp(statement.getTimestamp());
        expected.setColumnVisibility(new byte[0]);
        
        assertEquals(expected, statement);
    }
    
}
