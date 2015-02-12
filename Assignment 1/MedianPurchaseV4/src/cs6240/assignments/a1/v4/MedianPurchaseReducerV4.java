package cs6240.assignments.a1.v4;

/**
 * @Purpose: MedianPurchaseReducer Reducer for finding median of purchases done in each category
 *           example.
 * @FileName MedianPurchaseReducerV4.java
 * @author Pramod Khare
 * @Created Wed 21 Jan 2015 09:00:00 AM EST
 * @Modified
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MedianPurchaseReducerV4 extends Reducer<Text, FloatWritable, Text, FloatWritable> {

    @Override
    public void reduce(Text key, Iterable<FloatWritable> values, Context context)
            throws IOException, InterruptedException {

        // Get all the values in the list in a temporary list
        List<Float> allPurchaseValues = new ArrayList<Float>();

        for (FloatWritable value : values) {
            allPurchaseValues.add(value.get());
        }

        // Sort the list to find median value
        Collections.sort(allPurchaseValues);

        float medianValue;
        // Median of purchase-prices
        if ((allPurchaseValues.size() & 1) == 0) {
            float value1 = allPurchaseValues.get((int) (allPurchaseValues.size() / 2));
            float value2 = allPurchaseValues.get((int) ((allPurchaseValues.size() / 2) - 1));
            medianValue = (value1 + value2) / 2;
        } else {
            medianValue = allPurchaseValues.get((int) (allPurchaseValues.size() / 2));
        }

        context.write(key, new FloatWritable(medianValue));
    }
}
