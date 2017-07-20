package org.apache.rya.rdftriplestore.inference;
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.rdftriplestore.inference.HasValueVisitor;
import org.apache.rya.rdftriplestore.inference.InferenceEngine;
import org.apache.rya.rdftriplestore.utils.FixedStatementPattern;
import org.junit.Assert;
import org.junit.Test;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.ProjectionElem;
import org.openrdf.query.algebra.ProjectionElemList;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Union;
import org.openrdf.query.algebra.Var;

public class HasValueVisitorTest {
    private final AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();
    private final ValueFactory vf = new ValueFactoryImpl();

    private final URI chordate = vf.createURI("urn:Chordate");
    private final URI vertebrate = vf.createURI("urn:Vertebrate");
    private final URI mammal = vf.createURI("urn:Mammal");
    private final URI tunicate = vf.createURI("urn:Tunicate");
    private final URI hasCharacteristic = vf.createURI("urn:anatomicalCharacteristic");
    private final URI notochord = vf.createURI("urn:notochord");
    private final URI skull = vf.createURI("urn:skull");
    private final URI belongsTo = vf.createURI("urn:belongsToTaxon");
    private final URI chordata = vf.createURI("urn:Chordata");

    @Test
    public void testRewriteTypePattern() throws Exception {
        // Configure a mock instance engine with an ontology:
        final InferenceEngine inferenceEngine = mock(InferenceEngine.class);
        Map<URI, Set<Value>> vertebrateValues = new HashMap<>();
        vertebrateValues.put(hasCharacteristic, new HashSet<>());
        vertebrateValues.put(belongsTo, new HashSet<>());
        vertebrateValues.get(hasCharacteristic).add(notochord);
        vertebrateValues.get(hasCharacteristic).add(skull);
        vertebrateValues.get(belongsTo).add(chordata);
        when(inferenceEngine.getHasValueByType(vertebrate)).thenReturn(vertebrateValues);
        // Query for a specific type and rewrite using the visitor:
        final Projection query = new Projection(
                new StatementPattern(new Var("s"), new Var("p", RDF.TYPE), new Var("o", vertebrate)),
                new ProjectionElemList(new ProjectionElem("s", "subject")));
        query.visit(new HasValueVisitor(conf, inferenceEngine));
        // Expected structure: two nested unions whose members are (in some order) the original
        // statement pattern and two joins, one for each unique property involved in a relevant
        // restriction. Each join should be between a StatementPattern for the property and a
        // FixedStatementPattern providing the value(s).
        // Collect the arguments to the unions, ignoring nesting order:
        Assert.assertTrue(query.getArg() instanceof Union);
        final Union union1 = (Union) query.getArg();
        final Set<TupleExpr> unionArgs = new HashSet<>();
        if (union1.getLeftArg() instanceof Union) {
            unionArgs.add(((Union) union1.getLeftArg()).getLeftArg());
            unionArgs.add(((Union) union1.getLeftArg()).getRightArg());
            unionArgs.add(union1.getRightArg());
        }
        else {
            Assert.assertTrue(union1.getRightArg() instanceof Union);
            unionArgs.add(union1.getLeftArg());
            unionArgs.add(((Union) union1.getRightArg()).getLeftArg());
            unionArgs.add(((Union) union1.getRightArg()).getRightArg());
        }
        // There should be one StatementPattern and two joins with structure Join(FSP, SP):
        final StatementPattern directSP = new StatementPattern(new Var("s"), new Var("p", RDF.TYPE), new Var("o", vertebrate));
        StatementPattern actualSP = null;
        FixedStatementPattern hasCharacteristicFSP = null;
        FixedStatementPattern belongsToFSP = null;
        for (TupleExpr arg : unionArgs) {
            if (arg instanceof StatementPattern) {
                actualSP = (StatementPattern) arg;
            }
            else {
                Assert.assertTrue(arg instanceof Join);
                final Join join = (Join) arg;
                Assert.assertTrue(join.getLeftArg() instanceof FixedStatementPattern);
                Assert.assertTrue(join.getRightArg() instanceof StatementPattern);
                final FixedStatementPattern fsp = (FixedStatementPattern) join.getLeftArg();
                final StatementPattern sp = (StatementPattern) join.getRightArg();
                // Should join FSP([unused], property, ?value) with SP(subject, property, ?value)
                Assert.assertEquals(directSP.getSubjectVar(), sp.getSubjectVar());
                Assert.assertEquals(fsp.getPredicateVar(), sp.getPredicateVar());
                Assert.assertEquals(fsp.getObjectVar(), sp.getObjectVar());
                if (hasCharacteristic.equals(fsp.getPredicateVar().getValue())) {
                    hasCharacteristicFSP = fsp;
                }
                else if (belongsTo.equals(fsp.getPredicateVar().getValue())) {
                    belongsToFSP = fsp;
                }
                else {
                    Assert.fail("Unexpected property variable in rewritten query: " + fsp.getPredicateVar());
                }
            }
        }
        Assert.assertEquals(directSP, actualSP);
        Assert.assertNotNull(hasCharacteristicFSP);
        Assert.assertNotNull(belongsToFSP);
        // Verify the expected FSPs for the appropriate properties:
        Assert.assertEquals(2, hasCharacteristicFSP.statements.size());
        Assert.assertTrue(hasCharacteristicFSP.statements.contains(vf.createStatement(vertebrate, hasCharacteristic, skull)));
        Assert.assertTrue(hasCharacteristicFSP.statements.contains(vf.createStatement(vertebrate, hasCharacteristic, notochord)));
        Assert.assertEquals(1, belongsToFSP.statements.size());
        Assert.assertTrue(belongsToFSP.statements.contains(vf.createStatement(vertebrate, belongsTo, chordata)));
    }

    @Test
    public void testRewriteValuePattern() throws Exception {
        // Configure a mock inference engine with an ontology:
        final InferenceEngine inferenceEngine = mock(InferenceEngine.class);
        Map<Resource, Set<Value>> typeToCharacteristic = new HashMap<>();
        Set<Value> chordateCharacteristics = new HashSet<>();
        Set<Value> vertebrateCharacteristics = new HashSet<>();
        chordateCharacteristics.add(notochord);
        vertebrateCharacteristics.addAll(chordateCharacteristics);
        vertebrateCharacteristics.add(skull);
        typeToCharacteristic.put(chordate, chordateCharacteristics);
        typeToCharacteristic.put(tunicate, chordateCharacteristics);
        typeToCharacteristic.put(vertebrate, vertebrateCharacteristics);
        typeToCharacteristic.put(mammal, vertebrateCharacteristics);
        when(inferenceEngine.getHasValueByProperty(hasCharacteristic)).thenReturn(typeToCharacteristic);
        // Query for a specific type and rewrite using the visitor:
        final Projection query = new Projection(
                new StatementPattern(new Var("s"), new Var("p", hasCharacteristic), new Var("o")),
                new ProjectionElemList(new ProjectionElem("s", "subject"), new ProjectionElem("o", "characteristic")));
        query.visit(new HasValueVisitor(conf, inferenceEngine));
        // Expected structure: Union(Join(FSP, SP), [original SP])
        Assert.assertTrue(query.getArg() instanceof Union);
        final Union union = (Union) query.getArg();
        final StatementPattern originalSP = new StatementPattern(new Var("s"), new Var("p", hasCharacteristic), new Var("o"));
        Join join;
        if (union.getLeftArg() instanceof Join) {
            join = (Join) union.getLeftArg();
            Assert.assertEquals(originalSP, union.getRightArg());
        }
        else {
            Assert.assertTrue(union.getRightArg() instanceof Join);
            join = (Join) union.getRightArg();
            Assert.assertEquals(originalSP, union.getLeftArg());
        }
        Assert.assertTrue(join.getLeftArg() instanceof FixedStatementPattern);
        Assert.assertTrue(join.getRightArg() instanceof StatementPattern);
        final FixedStatementPattern fsp = (FixedStatementPattern) join.getLeftArg();
        final StatementPattern sp = (StatementPattern) join.getRightArg();
        // Verify join: FSP{ ?t _ ?originalObjectVar } JOIN { ?originalSubjectVar rdf:type ?t }
        Assert.assertEquals(originalSP.getSubjectVar(), sp.getSubjectVar());
        Assert.assertEquals(RDF.TYPE, sp.getPredicateVar().getValue());
        Assert.assertEquals(fsp.getSubjectVar(), sp.getObjectVar());
        Assert.assertEquals(originalSP.getObjectVar(), fsp.getObjectVar());
        // Verify FSP: should provide (type, value) pairs
        final Set<Statement> expectedStatements = new HashSet<>();
        final URI fspPred = (URI) fsp.getPredicateVar().getValue();
        expectedStatements.add(vf.createStatement(chordate, fspPred, notochord));
        expectedStatements.add(vf.createStatement(tunicate, fspPred, notochord));
        expectedStatements.add(vf.createStatement(vertebrate, fspPred, notochord));
        expectedStatements.add(vf.createStatement(mammal, fspPred, notochord));
        expectedStatements.add(vf.createStatement(vertebrate, fspPred, skull));
        expectedStatements.add(vf.createStatement(mammal, fspPred, skull));
        final Set<Statement> actualStatements = new HashSet<>(fsp.statements);
        Assert.assertEquals(expectedStatements, actualStatements);
    }
}
