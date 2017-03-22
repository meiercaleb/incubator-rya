package org.apache.rya.spark.query;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.ProjectionElem;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;

import com.google.common.base.Functions;
import com.google.common.collect.Sets;

public class SparqlToSQLQueryConverter extends QueryModelVisitorBase<RuntimeException> {

    private StringBuilder builder;
    private Map<StatementPattern, String> spTableNameMap;
    private Map<String, String> varToSpNameMap;
    private Map<String, String> varToSpNameMapVisited;
    private StatementPattern lastSp;
    private String contextName;
    private boolean contextNameSet = false;

    public SparqlToSQLQueryConverter() {
    }

    public String convertSparqlToSQL(TupleExpr query, Map<StatementPattern, String> spTableNameMap) throws MalformedQueryException {
        this.spTableNameMap = spTableNameMap;
        buildVarToSpNameMap(spTableNameMap.keySet());
        varToSpNameMapVisited = new HashMap<>();
        builder = new StringBuilder();
        query.visit(this);
        return builder.toString();
    }

    private void buildVarToSpNameMap(Set<StatementPattern> patterns) {
        varToSpNameMap = new HashMap<>();
        for (StatementPattern pattern : patterns) {
            List<Var> vars = pattern.getVarList();
            for (Var var : vars) {
                String name = var.getName();
                if (!var.isConstant() && !varToSpNameMap.containsKey(name)) {
                    varToSpNameMap.put(name, spTableNameMap.get(pattern) + "." + name);
                }
            }
        }
    }

    public void meet(StatementPattern node) {
        if (!contextNameSet) {
            Var contextNode = node.getContextVar();
            if (contextNode != null) {
                if (contextNode.getValue() == null) {
                    contextName = contextNode.getName();
                    contextNameSet = true;
                }
            } else {
                contextName = "context";
                contextNameSet = true;
            }
        }
        String tableName = spTableNameMap.get(node);
        Set<String> vars = node.getAssuredBindingNames().stream().filter(x -> !x.startsWith("-const-")).collect(Collectors.toSet());
        if(contextNameSet && !varToSpNameMapVisited.containsKey(contextName)) {
            varToSpNameMapVisited.put(contextName, tableName);
        }
        for(String var: vars) {
            if(!varToSpNameMapVisited.containsKey(var)) {
                varToSpNameMapVisited.put(var, tableName);
            }
        }
        
        builder.append(tableName);
    }

    public void meet(Join node) {
        TupleExpr left = node.getLeftArg();
        StatementPattern right = (StatementPattern) node.getRightArg();
        left.visit(this);
        SortedSet<String> commonVar = new TreeSet<>(Sets.intersection(left.getAssuredBindingNames(), right.getAssuredBindingNames()));
        String rightSpTableName = spTableNameMap.get(right);
        builder.append(" JOIN ");
        right.visit(this);
        builder.append(" ON ");
        if (contextNameSet) {
            commonVar.add(contextName);
        }
        for (String var : commonVar) {
            if (!var.startsWith("-const-")) {
                String leftSpTableName = varToSpNameMapVisited.get(var);
                builder.append(leftSpTableName + "." + var + " = " + rightSpTableName + "." + var);
                builder.append(" AND ");
            }
        }
        builder.delete(builder.length() - 5, builder.length());
    }

    public void meet(Projection node) {
        builder.append("SELECT ");
        List<ProjectionElem> elements = node.getProjectionElemList().getElements();
        for (ProjectionElem element : elements) {
            element.visit(this);
        }
        // delete extra comma and space
        builder.delete(builder.length() - 2, builder.length());
        builder.append(" FROM ");
        node.getArg().visit(this);
        ;
    }

    public void meet(ProjectionElem node) {
        builder.append(varToSpNameMap.get(node.getSourceName()) + " AS " + node.getSourceName() + ", ");
    }

    public void meetNode(QueryModelNode node) {
        throw new IllegalArgumentException("Only SPARQL queries with Joins, Projections, and StatementPatterns are supported.");
    }

}
