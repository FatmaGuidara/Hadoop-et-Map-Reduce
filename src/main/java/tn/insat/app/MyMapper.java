package tn.insat.app;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<Object, Text, Text, DoubleWritable> {

    private DoubleWritable writeCost = new DoubleWritable();
    private Text word = new Text();

    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String row = value.toString();
        String[] cols = row.split("\t");
        String magasin = cols[2];

        double cout = Double.parseDouble(cols[4]);
        word.set(magasin);
        writeCost.set(cout);
        context.write(word, writeCost);
    }
}
