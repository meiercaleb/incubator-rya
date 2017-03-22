package org.apache.rya.accumulo.spark.client;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.parser.sparql.SPARQLParser;

public class TestClient {

    public static void main(String[] args) throws FileNotFoundException, IOException, MalformedQueryException {
        
        Map<String, String> queries = getQueries(new File("/home/cloudera/Desktop/SparkBenchmarking/LUBM_part_queries.txt"));
        System.out.println(queries);
        
        String query = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#> SELECT ?X ?Y ?Z WHERE {?X rdf:type ub:GraduateStudent . ?Y rdf:type ub:University . ?Z rdf:type ub:Department . ?X ub:memberOf ?Z . ?Z ub:subOrganizationOf ?Y . ?X ub:undergraduateDegreeFrom ?Y}"; 
        SPARQLParser parser = new SPARQLParser();
//        for(Map.Entry<String, String> entry: queries.entrySet()) {
//            parser.parseQuery(entry.getValue(), null);
//        }
        parser.parseQuery(query, null);
    }
    
    private static Map<String, String> getQueries(File queryFile) throws FileNotFoundException, IOException {
        Map<String, String> queries = new HashMap<>();
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
                
                if(next.startsWith("Query")) {
                    key = next;
                    continue;
                }
                builder.append(next);
                builder.append(" ");
            }
            //add last query
            queries.put(key, builder.toString());
            
            return queries;
        }
    }

}
