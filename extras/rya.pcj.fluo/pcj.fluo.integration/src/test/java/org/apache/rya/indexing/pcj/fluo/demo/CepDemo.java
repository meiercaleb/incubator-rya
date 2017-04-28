package org.apache.rya.indexing.pcj.fluo.demo;

import static java.util.Objects.requireNonNull;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.core.client.FluoClientImpl;
import org.apache.fluo.recipes.test.FluoITHelper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.rya.api.client.InstanceDoesNotExistException;
import org.apache.rya.api.client.RyaClient;
import org.apache.rya.api.client.RyaClientException;
import org.apache.rya.api.client.accumulo.AccumuloConnectionDetails;
import org.apache.rya.api.client.accumulo.AccumuloRyaClientFactory;
import org.apache.rya.api.client.accumulo.FluoClientFactory;
import org.apache.rya.indexing.pcj.fluo.KafkaExportITBase;
import org.apache.rya.indexing.pcj.fluo.api.CreatePcj;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQuery;
import org.apache.rya.indexing.pcj.storage.PcjException;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSet;
import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.impl.MapBindingSet;
import org.openrdf.repository.sail.SailRepositoryConnection;

import com.google.common.collect.Sets;

public class CepDemo extends KafkaExportITBase {

    private static final ValueFactory vf = new ValueFactoryImpl();

    @Test
    public void constructQueryChainedToAgg() throws Exception {
        // A query that groups what is aggregated by one of the keys.
        final String construct = "CONSTRUCT { ?x <urn:type> <urn:grandMother> . ?z <urn:grandDaughterOf> ?x } WHERE { "
                + "?x <urn:motherOf> ?y. ?y <urn:motherOf> ?z. ?z <urn:type> <urn:female> }";

        final String aggregation = "SELECT ?x (count(?y) as ?totalGrandDaughters) { ?x <urn:type> <urn:grandMother> . "
                + "?y <urn:grandDaughterOf> ?x } GROUP BY ?x";

        final Collection<Statement> statements = Sets.newHashSet(
                // Betty's granchildren
                vf.createStatement(vf.createURI("urn:Betty"), vf.createURI("urn:motherOf"), vf.createURI("urn:Suzie")),
                vf.createStatement(vf.createURI("urn:Suzie"), vf.createURI("urn:motherOf"), vf.createURI("urn:Joan")),
                vf.createStatement(vf.createURI("urn:Joan"), vf.createURI("urn:type"), vf.createURI("urn:female")),
                vf.createStatement(vf.createURI("urn:Suzie"), vf.createURI("urn:motherOf"), vf.createURI("urn:Sara")),
                vf.createStatement(vf.createURI("urn:Sara"), vf.createURI("urn:type"), vf.createURI("urn:female")),
                vf.createStatement(vf.createURI("urn:Suzie"), vf.createURI("urn:motherOf"), vf.createURI("urn:Evan")),
                vf.createStatement(vf.createURI("urn:Evan"), vf.createURI("urn:type"), vf.createURI("urn:male")),
                // Jennifer's grandchildren
                vf.createStatement(vf.createURI("urn:Jennifer"), vf.createURI("urn:motherOf"), vf.createURI("urn:Debbie")),
                vf.createStatement(vf.createURI("urn:Debbie"), vf.createURI("urn:motherOf"), vf.createURI("urn:Carol")),
                vf.createStatement(vf.createURI("urn:Carol"), vf.createURI("urn:type"), vf.createURI("urn:female")),
                vf.createStatement(vf.createURI("urn:Debbie"), vf.createURI("urn:motherOf"), vf.createURI("urn:Emily")),
                vf.createStatement(vf.createURI("urn:Emily"), vf.createURI("urn:type"), vf.createURI("urn:female")),
                vf.createStatement(vf.createURI("urn:Debbie"), vf.createURI("urn:motherOf"), vf.createURI("urn:Isabelle")),
                vf.createStatement(vf.createURI("urn:Isabelle"), vf.createURI("urn:type"), vf.createURI("urn:female")));
        
        createConstructPcj(construct);
        String pcjId = createAggregationPcj(aggregation);
        
        loadData(statements);
        
        super.getMiniFluo().waitForObservers();
        FluoITHelper.printFluoTable(new FluoClientImpl(super.getFluoConfiguration()));
        
        System.out.println(readGroupedResults(pcjId, new VariableOrder("x")));

    }

    private void createConstructPcj(String sparql) throws MalformedQueryException, PcjException {
        CreatePcj createPcj = new CreatePcj();
        FluoClient client = new FluoClientImpl(super.getFluoConfiguration());
        FluoQuery fluoQuery = createPcj.createFluoPcj(client, sparql);
    }

    private String createAggregationPcj(String sparql) throws InstanceDoesNotExistException, RyaClientException {
        // Register the PCJ with Rya.
        final Instance accInstance = super.getAccumuloConnector().getInstance();
        final Connector accumuloConn = super.getAccumuloConnector();

        final RyaClient ryaClient = AccumuloRyaClientFactory.build(new AccumuloConnectionDetails(ACCUMULO_USER,
                ACCUMULO_PASSWORD.toCharArray(), accInstance.getInstanceName(), accInstance.getZooKeepers()), accumuloConn);

        final String pcjId = ryaClient.getCreatePCJ().createPCJ(RYA_INSTANCE_NAME, sparql);
        return pcjId;
    }

    private void loadData(final Collection<Statement> statements) throws Exception {

        // Write the data to Rya.
        final SailRepositoryConnection ryaConn = getRyaSailRepository().getConnection();
        ryaConn.begin();
        ryaConn.add(statements);
        ryaConn.commit();
        ryaConn.close();

        // Wait for the Fluo application to finish computing the end result.
        super.getMiniFluo().waitForObservers();
    }

    private Set<VisibilityBindingSet> readAllResults(final String pcjId) throws Exception {
        requireNonNull(pcjId);

        // Read all of the results from the Kafka topic.
        final Set<VisibilityBindingSet> results = new HashSet<>();

        try (final KafkaConsumer<Integer, VisibilityBindingSet> consumer = makeConsumer(pcjId)) {
            final ConsumerRecords<Integer, VisibilityBindingSet> records = consumer.poll(5000);
            final Iterator<ConsumerRecord<Integer, VisibilityBindingSet>> recordIterator = records.iterator();
            while (recordIterator.hasNext()) {
                results.add(recordIterator.next().value());
            }
        }

        return results;
    }
    
    private Set<VisibilityBindingSet> readGroupedResults(final String pcjId, final VariableOrder groupByVars) {
        requireNonNull(pcjId);

        // Read the results from the Kafka topic. The last one for each set of Group By values is an aggregation result.
        // The key in this map is a Binding Set containing only the group by variables.
        final Map<BindingSet, VisibilityBindingSet> results = new HashMap<>();

        try(final KafkaConsumer<Integer, VisibilityBindingSet> consumer = makeConsumer(pcjId)) {
            final ConsumerRecords<Integer, VisibilityBindingSet> records = consumer.poll(5000);
            final Iterator<ConsumerRecord<Integer, VisibilityBindingSet>> recordIterator = records.iterator();
            while (recordIterator.hasNext()) {
                final VisibilityBindingSet visBindingSet = recordIterator.next().value();

                final MapBindingSet key = new MapBindingSet();
                for(final String groupByBar : groupByVars) {
                    key.addBinding( visBindingSet.getBinding(groupByBar) );
                }

                results.put(key, visBindingSet);
            }
        }

        return Sets.newHashSet( results.values() );
    }

}
