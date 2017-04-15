package org.apache.rya.api.domain;

import java.util.ArrayList;
import java.util.List;

import org.apache.rya.api.domain.RyaStatement;

public class RyaBundle {

    private String id;
    private List<RyaStatement> statements;
    
    public RyaBundle(String id) {
        this.id = id;
        this.statements = new ArrayList<RyaStatement>();
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
    
}