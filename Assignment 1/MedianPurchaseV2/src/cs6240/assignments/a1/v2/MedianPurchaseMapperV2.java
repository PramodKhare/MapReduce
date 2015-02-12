package cs6240.assignments.a1.v2;

/**
 * @Purpose: MedianPurchaseMapper Mapper for finding median of purchases done in each category.
 * 
 * @FileName MedianPurchaseMapperV2.java
 * @author Pramod Khare
 * @Created Mon 19 Jan 2015 11:45:24 PM EST
 * @Modified
 */

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MedianPurchaseMapperV2 extends Mapper<LongWritable, Text, Text, FloatWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException,
            InterruptedException {
        String line = value.toString();

        // Split the line along the tab characters
        Pattern p = Pattern.compile("\\t+");
        try {
            String[] tokens = p.split(line);
            // Fourth Token is Category name
            // Fifth Token is purchase price for that catogory, parse it to float
            context.write(new Text(tokens[3]), new FloatWritable(Float.valueOf(tokens[4])));
        } catch (final Throwable t) {
            // ignore the invalid inputs
        }
    }
}
