package org.apache.rya.indexing.pcj.storage;

import java.util.Objects;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;

import com.google.common.base.Preconditions;

public class PeriodicQueryStorageMetadata {

    private String sparql;
    private VariableOrder varOrder;

    public PeriodicQueryStorageMetadata(String sparql, VariableOrder varOrder) {
        Preconditions.checkNotNull(sparql);
        Preconditions.checkNotNull(varOrder);
        this.sparql = sparql;
        this.varOrder = varOrder;
    }
    
    public PeriodicQueryStorageMetadata(PcjMetadata metadata) {
        this(metadata.getSparql(), metadata.getVarOrders().iterator().next());
    }
    

    public String getSparql() {
        return sparql;
    }
    
    public VariableOrder getVariableOrder() {
        return varOrder;
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(sparql, varOrder);
    }
   
    @Override
    public boolean equals(final Object o) {
        if (o == this) {
            return true;
        }

        if (o instanceof PeriodicQueryStorageMetadata) {
                PeriodicQueryStorageMetadata metadata = (PeriodicQueryStorageMetadata) o;
                return new EqualsBuilder().append(sparql, metadata.sparql).append(varOrder, metadata.varOrder).isEquals();
        }

        return false;
    }
    
    @Override
    public String toString() {
        return new StringBuilder()
                .append("PeriodicQueryStorageMetadata {\n")
                .append("    SPARQL: " + sparql + "\n")
                .append("    Variable Order: " + varOrder + "\n")
                .append("}")
                .toString();
    }
    
    
}
