package org.apache.rya.indexing.pcj.fluo.app;
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
import java.io.UnsupportedEncodingException;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.log4j.Logger;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSet;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.Var;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 * This class projects a VisibilityBindingSet onto a RyaStatement. The Binding {@link Value}s
 * that get projected onto subject, predicate and object are indicated by the
 * names {@link ConstructProjection#getSubjectSourceVar()},
 * {@link ConstructProjection#getPredicateSourceVar()} and
 * {@link ConstructProjection#getObjectSourceVar()} and must satisfy standard
 * RDF constraints for RDF subjects, predicates and objects. The purpose of
 * projecting {@link BindingSet}s in this way is to provide functionality for
 * SPARQL Construct queries which create RDF statements from query results.
 *
 */
public class ConstructProjection {

    private static final Logger log = Logger.getLogger(ConstructProjection.class);
    private Var subjectSourceVar;
    private Var predicateSourceVar;
    private Var objectSourceVar;
    public static final String subject = "subject";
    public static final String predicate = "predicate";
    public static final String object = "object";

    public ConstructProjection(Var subjectVar, Var predicateVar, Var objectVar) {
        Preconditions.checkNotNull(subjectVar);
        Preconditions.checkNotNull(predicateVar);
        Preconditions.checkNotNull(objectVar);
        if (subjectVar.isConstant()) {
            Preconditions.checkArgument(subjectVar.getValue() != null);
        }
        if (predicateVar.isConstant()) {
            Preconditions.checkArgument(predicateVar.getValue() != null);
        }
        if (objectVar.isConstant()) {
            Preconditions.checkArgument(objectVar.getValue() != null);
        }
        this.subjectSourceVar = subjectVar;
        this.predicateSourceVar = predicateVar;
        this.objectSourceVar = objectVar;
    }

    public ConstructProjection(StatementPattern pattern) {
        this(pattern.getSubjectVar(), pattern.getPredicateVar(), pattern.getObjectVar());
    }

    /**
     * Returns a Var with info about the Value projected onto the RyaStatement subject. 
     *  If the org.openrdf.query.algebra.Var returned by this method is
     * not constant (as indicated by {@link Var#isConstant()}, then
     * {@link Var#getName()} is the Binding name that gets projected. If the Var
     * is constant, then {@link Var#getValue()} is assigned to the subject
     * 
     * @return {@link org.openrdf.query.algebra.Var} containing info about
     *         Binding that gets projected onto the subject 
     */
    public Var getSubjectSourceVar() {
        return subjectSourceVar;
    }

    /**
     * Returns a Var with info about the Value projected onto the RyaStatement predicate.  
     * If the org.openrdf.query.algebra.Var returned by this method is
     * not constant (as indicated by {@link Var#isConstant()}, then
     * {@link Var#getName()} is the Binding name that gets projected. If the Var
     * is constant, then {@link Var#getValue()} is assigned to the predicate
     * 
     * @return {@link org.openrdf.query.algebra.Var} containing info about
     *         Binding that gets projected onto the predicate 
     */
    public Var getPredicateSourceVar() {
        return predicateSourceVar;
    }

    /**
     * Returns a Var with info about the Value projected onto the RyaStatement object. 
     * If the org.openrdf.query.algebra.Var returned by this method is
     * not constant (as indicated by {@link Var#isConstant()}, then
     * {@link Var#getName()} is the Binding name that gets projected. If the Var
     * is constant, then {@link Var#getValue()} is assigned to the object
     * 
     * @return {@link org.openrdf.query.algebra.Var} containing info about
     *         Binding that gets projected onto the object
     */
    public Var getObjectSourceVar() {
        return objectSourceVar;
    }
    
    /**
     * @return SubjectPattern representation of this ConstructProjection containing the
     * {@link ConstructProjection#subjectSourceVar}, {@link ConstructProjection#predicateSourceVar},
     * {@link ConstructProjection#objectSourceVar}
     */
    public StatementPattern getStatementPatternRepresentation() {
        return new StatementPattern(subjectSourceVar, predicateSourceVar, objectSourceVar);
    }

    /**
     * Projects a given BindingSet onto a RyaStatement. The subject, predicate, and
     * object are extracted from the input VisibilityBindingSet (if the
     * subjectSourceVar, predicateSourceVar, objectSourceVar is resp.
     * non-constant) and from the Var Value itself (if subjectSourceVar,
     * predicateSource, objectSourceVar is resp. constant).
     * 
     * 
     * @param vBs
     *            - Visibility BindingSet that gets projected onto an RDF
     *            Statement BindingSet with Binding names subject, predicate and
     *            object
     * @return - RyaStatement whose values are determined by
     *         {@link ConstructProjection#getSubjectSourceVar()},
     *         {@link ConstructProjection#getPredicateSourceVar()},
     *         {@link ConstructProjection#getObjectSourceVar()}.
     * 
     */
    public RyaStatement projectBindingSet(VisibilityBindingSet vBs) {
        Value subj = null;
        Value pred = null;
        Value obj = null;
        if (subjectSourceVar.isConstant()) {
            subj = subjectSourceVar.getValue();
        } else {
            subj = vBs.getValue(subjectSourceVar.getName());
        }
        if (predicateSourceVar.isConstant()) {
            pred = predicateSourceVar.getValue();
        } else {
            pred = vBs.getValue(predicateSourceVar.getName());
        }
        if (objectSourceVar.isConstant()) {
            obj = objectSourceVar.getValue();
        } else {
            obj = vBs.getValue(objectSourceVar.getName());
        }
        Preconditions.checkNotNull(subj);
        Preconditions.checkNotNull(pred);
        Preconditions.checkNotNull(obj);
        Preconditions.checkArgument(subj instanceof Resource);
        Preconditions.checkArgument(pred instanceof URI);
        
        RyaURI subjType = RdfToRyaConversions.convertResource((Resource) subj);
        RyaURI predType = RdfToRyaConversions.convertURI((URI) pred);
        RyaType objectType = RdfToRyaConversions.convertValue(obj);
        
        RyaStatement statement = new RyaStatement(subjType, predType, objectType);
        try {
            statement.setColumnVisibility(vBs.getVisibility().getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            log.trace("Unable to decode column visibility.  RyaStatement being created without column visibility.");
        }
        return statement;
    }
    
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o instanceof ConstructProjection) {
            ConstructProjection projection = (ConstructProjection) o;
            return new EqualsBuilder().append(this.subjectSourceVar, projection.subjectSourceVar)
                    .append(this.predicateSourceVar, projection.predicateSourceVar).append(this.objectSourceVar, projection.objectSourceVar)
                    .isEquals();
        }
        return false;

    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(this.subjectSourceVar, this.predicateSourceVar, this.objectSourceVar);
    }
    
    @Override
    public String toString() {
        return new StringBuilder()
                .append("Construct Projection {\n")
                .append("   Subject Var: " + subjectSourceVar + "\n")
                .append("   Predicate Var: " + predicateSourceVar + "\n")
                .append("   Object Var: " + objectSourceVar + "\n")
                .append("}")
                .toString();
    }

}
