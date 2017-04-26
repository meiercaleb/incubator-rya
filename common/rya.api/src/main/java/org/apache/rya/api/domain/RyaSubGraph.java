package org.apache.rya.api.domain;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Objects;

public class RyaSubGraph {

    private String id;
    private List<RyaStatement> statements;
    
    public RyaSubGraph(String id) {
        this.id = id;
        this.statements = new ArrayList<RyaStatement>();
    }
    
    public RyaSubGraph(String id, List<RyaStatement> statements) {
        this.id = id;
        this.statements = statements;
    }

    public String getId() {
        return id;
    }
    
    public List<RyaStatement> getStatements() {
        return statements;
    }
    
    public void setId(String id) {
        this.id = id;
    }
    
    public void setStatements(List<RyaStatement> statements) {
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
            return Objects.equal(this.id, ((RyaSubGraph) other).id) && Objects.equal(this.statements, bundle.statements);
        }
        
        return false;
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(this.id, this.statements);
    }
    
}