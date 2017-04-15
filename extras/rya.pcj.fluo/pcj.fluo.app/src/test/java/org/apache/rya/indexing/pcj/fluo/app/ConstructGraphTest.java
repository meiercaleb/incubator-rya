package org.apache.rya.indexing.pcj.fluo.app;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Set;

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

public class ConstructGraphTest {

    private ValueFactory vf = new ValueFactoryImpl();
    
    @Test
    public void testConstructGraph() throws MalformedQueryException {
        String query = "select ?x where { ?x <uri:talksTo> <uri:Bob>. ?y <uri:worksAt> ?z }";

        SPARQLParser parser = new SPARQLParser();
        ParsedQuery pq = parser.parseQuery(query, null);
        List<StatementPattern> patterns = StatementPatternCollector.process(pq.getTupleExpr());
        ConstructGraph graph = new ConstructGraph(patterns);

        QueryBindingSet bs = new QueryBindingSet();
        bs.addBinding("x", vf.createURI("uri:Joe"));
        bs.addBinding("y", vf.createURI("uri:Bob"));
        bs.addBinding("z", vf.createURI("uri:BurgerShack"));
        VisibilityBindingSet vBs = new VisibilityBindingSet(bs, "FOUO");
        Set<RyaStatement> statements = graph.createGraphFromBindingSet(vBs);

        System.out.println(statements);

        // RyaStatement expected = new RyaStatement(new RyaURI("uri:Joe"), new
        // RyaURI("uri:talksTo"), new RyaURI("uri:Bob"));
        // expected.setColumnVisibility("FOUO".getBytes("UTF-8"));
        // expected.setTimestamp(statement.getTimestamp());
        //
        // assertEquals(expected, statement);
    }
    
    @Test
    public void testConstructGraphSerializer() throws MalformedQueryException {
        
        String query = "select ?x where { ?x <uri:talksTo> <uri:Bob>. ?y <uri:worksAt> ?z }";

        SPARQLParser parser = new SPARQLParser();
        ParsedQuery pq = parser.parseQuery(query, null);
        List<StatementPattern> patterns = StatementPatternCollector.process(pq.getTupleExpr());
        ConstructGraph graph = new ConstructGraph(patterns);
        
        String constructString = ConstructGraphSerializer.toConstructString(graph);
        ConstructGraph deserialized = ConstructGraphSerializer.toConstructGraph(constructString);
        
        assertEquals(graph, deserialized);
        
    }
    
}
