package cs6240.assignments.a2.v4_old;

/**
 * @Purpose: MedianPurchaseReducer Reducer for finding median of purchases done in each category
 *           example.
 * @FileName MedianPurchaseReducerV4.java
 * @author Pramod Khare
 * @Created Mon 09 Feb 2015 12:59:24 PM EST
 * @Modified
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MedianPurchaseReducerV4 extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
            InterruptedException {

        // Get all the values in the list in a temporary list
        List<Integer> allPurchaseValues = new ArrayList<Integer>();

        for (IntWritable value : values) {
            allPurchaseValues.add(value.get());
        }

        // Sort the list to find median value
        Collections.sort(allPurchaseValues);

        context.write(key, new IntWritable(findMedianOfSortedList(allPurchaseValues)));
    }

    /**
     * Returns the Median value of given sorted list of values
     * 
     * @param prices
     * @return median value
     */
    private int findMedianOfSortedList(List<Integer> prices) {
        // Median of purchase-prices
        // If even or odd number of purchases
        if ((prices.size() & 1) == 0) { // Check if there are even number of purchase-prices
            int value1 = prices.get((int) (prices.size() / 2));
            int value2 = prices.get((int) ((prices.size() / 2) - 1));
            return (value1 + value2) / 2;
        } else {
            return prices.get((int) (prices.size() / 2));
        }
    }
}
