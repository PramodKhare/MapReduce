package cs6240.assignments.a2.v4_old;

/**
 * @Purpose: MedianPurchaseMapper Mapper for finding median of purchases done in each category.
 * 
 * @FileName MedianPurchaseMapperV4.java
 * @author Pramod Khare
 * @Created Mon 09 Feb 2015 12:59:24 PM EST
 * @Modified
 */

import java.io.IOException;
import java.util.Random;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MedianPurchaseMapperV4 extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final Pattern p = Pattern.compile("\\t+");

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException,
            InterruptedException {
        // Split the line along the tab characters
        try {
            String[] tokens = p.split(value.toString());
            // Random number between range 5 to 14
            calculateFibonacci(new Random().nextInt(10) + 5);

            context.write(new Text(tokens[3]), new IntWritable((int) Float.parseFloat(tokens[4])));
        } catch (final Throwable t) {
            // ignore the invalid inputs
        }
    }

    /**
     * Calculate Fibonacci for all values till limit number
     * 
     * @param limit
     */
    private static void calculateFibonacci(int limit) {
        for (int i = 0; i < limit; i++) {
            fib(i);
        }
    }

    /**
     * Author Wikipedia "Fibonacci Series"
     * 
     * @param limit - how many values to compute
     * @return the calculated fibonacci value
     */
    private static int fib(int limit) {
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
