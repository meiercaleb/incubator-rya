package org.apache.rya.indexing.pcj.fluo.app.query;

import org.apache.rya.indexing.pcj.fluo.app.util.PeriodicQueryUtil.PeriodicQueryNodeRelocator;
import org.apache.rya.indexing.pcj.fluo.app.util.PeriodicQueryUtil.PeriodicQueryNodeVisitor;
import org.junit.Test;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

public class PeriodicQueryUtilTest {

    @Test
    public void periodicNodePlacementTest() throws MalformedQueryException {

        String query = "prefix function: <http://org.apache.rya/function#> " //n
                + "prefix time: <http://www.w3.org/2006/time#> " //n
                + "prefix fn: <http://www.w3.org/2006/fn#> " //n
                + "select ?obs ?time ?lat where {" //n
                + "Filter(function:periodic(?time, 12.0, 6.0,time:hours)) " //n
                + "Filter(fn:test(?lat, 25)) " //n
                + "?obs <uri:hasTime> ?time. " //n
                + "?obs <uri:hasLattitude> ?lat }"; //n
        
        SPARQLParser parser = new SPARQLParser();
        ParsedQuery pq = parser.parseQuery(query, null);
        TupleExpr te = pq.getTupleExpr();
        System.out.println(te);
        te.visit(new PeriodicQueryNodeVisitor());
        System.out.println(te);
        te.visit(new PeriodicQueryNodeRelocator());
        System.out.println(te);
        
        

    }

}
