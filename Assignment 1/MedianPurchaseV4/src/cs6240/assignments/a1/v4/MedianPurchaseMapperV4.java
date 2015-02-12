package cs6240.assignments.a1.v4;

/**
 * @Purpose: MedianPurchaseMapper Mapper for finding median of purchases done in each category.
 * 
 * @FileName MedianPurchaseMapperV4.java
 * @author Pramod Khare
 * @Created Wed 21 Jan 2015 09:00:00 AM EST
 * @Modified
 */

import java.io.IOException;
import java.util.Random;
import java.util.regex.Pattern;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MedianPurchaseMapperV4 extends Mapper<LongWritable, Text, Text, FloatWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException,
            InterruptedException {
        String line = value.toString();

        // Split the line along the tab characters
        Pattern p = Pattern.compile("\\t+");
        try {
            String[] tokens = p.split(line);
            // Fourth Token is Category name
            String strCategoryName = tokens[3];
            // Fifth Token is purchase price for that catogory
            String strPurchasePrice = tokens[4];

            // float purchasePrice = Float.parseFloat(strPurchasePrice);
            float purchasePrice = Float.valueOf(strPurchasePrice);

            // Random number between range 5 to 10
            calculateFibonacci(new Random().nextInt(6) + 5);

            context.write(new Text(strCategoryName), new FloatWritable(purchasePrice));
        } catch (final Throwable t) {
            // ignore the invalid inputs
        }
    }

    private static int calculateFibonacci(int limit) {
        if (limit < 2) {
            return limit;
        }

        int i = 1, j = 0, result = 0;
        for (; limit > 1; limit--) {
            result = i + j;
            j = i;
            i = result;
        }
        return result;
    }
}
