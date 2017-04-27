package org.apache.rya.api.domain;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.base.Objects;
import com.google.common.collect.Sets;

public class RyaSubGraph {

    private String id;
    private Set<RyaStatement> statements;
    
    public RyaSubGraph(String id) {
        this.id = id;
        this.statements = new HashSet<>();
    }
    
    public RyaSubGraph(String id, Set<RyaStatement> statements) {
        this.id = id;
        this.statements = statements;
    }

    public String getId() {
        return id;
    }
    
    public Set<RyaStatement> getStatements() {
        return statements;
    }
    
    public void setId(String id) {
        this.id = id;
    }
    
    public void setStatements(Set<RyaStatement> statements) {
        this.statements = statements;
    }
    

    public void addStatement(RyaStatement statement){
        statements.add(statement);
    }
    
    @Override
    public boolean equals(Object other) {
        
        if(this == other) {
            return true;
        }
        
        if(other instanceof RyaSubGraph) {
            RyaSubGraph bundle = (RyaSubGraph) other;
            return Objects.equal(this.id, ((RyaSubGraph) other).id) && Objects.equal(this.statements,bundle.statements);
        }
        
        return false;
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(this.id, this.statements);
    }
    
    
    @Override
    public String toString() {
        return new StringBuilder().append("Rya Subgraph {\n").append("   Rya Subgraph ID: " + id + "\n")
                .append("   Rya Statements: " + statements + "\n").toString();
    }
    
}