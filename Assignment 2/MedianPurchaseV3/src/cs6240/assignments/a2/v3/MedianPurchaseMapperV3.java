package cs6240.assignments.a2.v3;

/**
 * @Purpose: MedianPurchaseMapper Mapper for finding median of purchases done in each category.
 * 
 * @FileName MedianPurchaseMapperV3.java
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

public class MedianPurchaseMapperV3 extends
        Mapper<LongWritable, Text, PurchaseCategoryPriceCombinationKey, IntWritable> {

    private static final Pattern p = Pattern.compile("\\t+");

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException,
            InterruptedException {
        // Split the line along the tab characters
        try {
            String[] tokens = p.split(value.toString());
            // Fourth Token is Category name i.e. tokens[3]
            // Fifth Token is purchase price for that category i.e. tokens[4]
            context.write(
                    new PurchaseCategoryPriceCombinationKey(tokens[3], (int) Float
                            .parseFloat(tokens[4])),
                    new IntWritable((int) Float.parseFloat(tokens[4])));
        } catch (final Throwable t) {
            t.printStackTrace();
            // ignore the invalid inputs
        }
    }
}
