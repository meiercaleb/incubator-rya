package org.apache.rya.indexing.pcj.matching;

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
 
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.rya.indexing.external.matching.ExternalSetMatcher;
import org.apache.rya.indexing.external.matching.QuerySegmentFactory;
import org.apache.rya.indexing.external.tupleSet.ExternalTupleSet;
import org.apache.rya.indexing.external.tupleSet.SimpleExternalTupleSet;
import org.junit.Assert;
import org.junit.Test;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.LeftJoin;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

public class JoinSegmentPCJMatcherTest {

    private final PCJExternalSetMatcherFactory pcjFactory = new PCJExternalSetMatcherFactory();
    private final QuerySegmentFactory<ExternalTupleSet> qFactory = new QuerySegmentFactory<ExternalTupleSet>();
    
	@Test
	public void testBasicSegment() throws MalformedQueryException {

		String query1 = ""//
				+ "SELECT ?e ?c ?l" //
				+ "{" //
				+ "  ?e a ?c . "//
				+ "  ?e <uri:talksTo> ?l  . "//
				+ "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
				+ "}";//

		String query2 = ""//
				+ "SELECT ?e ?c ?l" //
				+ "{" //
				+ "  ?e a ?c . "//
				+ "  ?e <uri:talksTo> ?l  . "//
				+ "}";//

		SPARQLParser parser = new SPARQLParser();
		ParsedQuery pq1 = parser.parseQuery(query1, null);
		ParsedQuery pq2 = parser.parseQuery(query2, null);
		TupleExpr te1 = pq1.getTupleExpr();
		TupleExpr te2 = pq2.getTupleExpr();
		Projection proj = (Projection) te1;
		Join join = (Join) proj.getArg();

		ExternalSetMatcher<ExternalTupleSet> jsm = pcjFactory.getMatcher(qFactory.getQuerySegment(join));
		SimpleExternalTupleSet pcj = new SimpleExternalTupleSet((Projection)te2);
		Assert.assertEquals(true, jsm.match(pcj));
		TupleExpr te = jsm.getQuery();
		Assert.assertEquals(new HashSet<QueryModelNode>(), jsm.getUnmatchedArgNodes());

		Set<QueryModelNode> qNodes = QueryNodeGatherer.getNodes(te);
		List<QueryModelNode> nodes = jsm.getOrderedNodes();
		Set<QueryModelNode> nodeSet = new HashSet<>();
		nodeSet.add(nodes.get(0));
		nodeSet.add(pcj);

		Assert.assertEquals(nodeSet, new HashSet<QueryModelNode>(nodes));
		Assert.assertEquals(nodeSet, qNodes);

	}


	@Test
	public void testBasicMatchWithFilter() throws Exception {

		String query1 = ""//
				+ "SELECT ?e ?c ?l" //
				+ "{" //
				+ " Filter(?e = <uri:Bob>)" //
				+ " Filter(?c = <uri:Lawyer>)" //
				+ "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
				+ "  ?e a ?c . "//
				+ "  ?e <uri:talksTo> ?l  . "//
				+ "}";//

		String query2 = ""//
				+ "SELECT ?e ?c ?l" //
				+ "{" //
				+ " Filter(?e = <uri:Bob>)" //
				+ " ?e a ?c . "//
				+ " ?e <uri:talksTo> ?l . "//
				+ "}";//

		SPARQLParser parser = new SPARQLParser();
		ParsedQuery pq1 = parser.parseQuery(query1, null);
		ParsedQuery pq2 = parser.parseQuery(query2, null);
		TupleExpr te1 = pq1.getTupleExpr();
		TupleExpr te2 = pq2.getTupleExpr();
		Projection proj = (Projection) te1;
		Filter filter = (Filter) proj.getArg();

		ExternalSetMatcher<ExternalTupleSet> jsm = pcjFactory.getMatcher(qFactory.getQuerySegment(filter));
		SimpleExternalTupleSet pcj = new SimpleExternalTupleSet((Projection)te2);
		Assert.assertEquals(true, jsm.match(pcj));
		TupleExpr te = jsm.getQuery();
		Assert.assertEquals(new HashSet<QueryModelNode>(), jsm.getUnmatchedArgNodes());

		Set<QueryModelNode> qNodes = QueryNodeGatherer.getNodes(te);
		List<QueryModelNode> nodes = jsm.getOrderedNodes();
		Set<QueryModelNode> nodeSet = new HashSet<>();
		nodeSet.add(nodes.get(0));
		nodeSet.add(pcj);
		nodeSet.add(nodes.get(1));

		Assert.assertEquals(nodeSet, new HashSet<QueryModelNode>(nodes));
		Assert.assertEquals(nodeSet, qNodes);

	}



	static class QueryNodeGatherer extends QueryModelVisitorBase<RuntimeException> {

		private static Set<QueryModelNode> nodes;

		public static Set<QueryModelNode> getNodes(TupleExpr te) {
			nodes = new HashSet<>();
			te.visit(new QueryNodeGatherer());
			return nodes;
		}


		@Override
		public void meetNode(QueryModelNode node) {
			if(node instanceof ExternalTupleSet) {
				nodes.add(node);
			}
			super.meetNode(node);
		}

		@Override
		public void meet(StatementPattern node) {
			nodes.add(node);
		}

		@Override
		public void meet(Filter node) {
			nodes.add(node.getCondition());
			super.meet(node);
		}

		@Override
		public void meet(LeftJoin node) {
			nodes.add(node);
		}

	}



}
