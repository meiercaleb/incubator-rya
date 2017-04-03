package org.apache.rya.indexing.pcj.fluo.app;

import java.util.Date;

import org.openrdf.model.Literal;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.vocabulary.XMLSchema;

public class TempTest {

    
    public static void main(String[] args) {
        
        Literal literal = new LiteralImpl( "2009-06-15T13:45:30", XMLSchema.DATETIME);
        Date val = literal.calendarValue().toGregorianCalendar().getTime();
        System.out.println(val);
        System.out.println(val.getTime());
        
    }
    
}
