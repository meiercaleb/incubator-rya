package org.apache.rya.accumulo.spark;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

public class StringRowWritable implements Writable {

    private Row row;
    
    public StringRowWritable() {};
    
    public StringRowWritable(Row row) {
        this.row = row;
    }
    
    public Row getRow() {
        return row;
    }
    
    public void setRow(Row row) {
        this.row = row;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        IntWritable size = new IntWritable();
        size.readFields(in);
        int n = size.get();
        Object[] entries = new String[n];
        for(int i = 0; i < n; i++) {
            Text text = new Text();
            text.readFields(in);
            entries[i] = text.toString();
        }
        
        setRow(RowFactory.create(entries));
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // TODO Auto-generated method stub
        IntWritable size = new IntWritable(row.size());
        size.write(out);
        for(int i = 0; i < row.size(); i++) {
            Text text = new Text(row.getString(i));
            text.write(out);
        }
    }
    

}
