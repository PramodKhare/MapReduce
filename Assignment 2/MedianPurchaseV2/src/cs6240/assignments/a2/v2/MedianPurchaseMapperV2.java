package cs6240.assignments.a2.v2;

/**
 * @Purpose: MedianPurchaseMapper Mapper for finding approximate median of purchases done in each
 *           category.
 * 
 * @FileName MedianPurchaseMapperV2.java
 * @author Pramod Khare
 * @Created Sun 08 Feb 2015 22:59:24 PM EST
 * @Modified
 */

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MedianPurchaseMapperV2 extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final Pattern p = Pattern.compile("\\t+");

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException,
            InterruptedException {
        // Split the line along the tab characters
        try {
            String[] tokens = p.split(value.toString());
            context.write(new Text(tokens[3]), new IntWritable((int) Float.parseFloat(tokens[4])));
        } catch (final Throwable t) {
            // ignore the invalid inputs
        }
    }
}
