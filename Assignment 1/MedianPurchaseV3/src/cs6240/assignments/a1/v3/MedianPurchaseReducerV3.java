package cs6240.assignments.a1.v3;

/**
 * @Purpose: MedianPurchaseReducer Reducer for finding median of purchases done in each category
 *           example.
 * @FileName MedianPurchaseReducerV3.java
 * @author Pramod Khare
 * @Created Tue 20 Jan 2015 01:45:24 PM EST
 * @Modified
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MedianPurchaseReducerV3 extends
        Reducer<PurchaseCategoryPriceCombinationKey, FloatWritable, Text, FloatWritable> {

    @Override
    public void reduce(PurchaseCategoryPriceCombinationKey key, Iterable<FloatWritable> values,
            Context context) throws IOException, InterruptedException {

        // Get all the values in the list in a temporary list
        final List<Float> allPurchaseValues = new ArrayList<Float>();

        for (FloatWritable value : values) {
            allPurchaseValues.add(value.get());
        }

        float medianValue;
        // Median of purchase-prices
        if ((allPurchaseValues.size() & 1) == 0) {
            float value1 = allPurchaseValues.get((int) (allPurchaseValues.size() / 2));
            float value2 = allPurchaseValues.get((int) ((allPurchaseValues.size() / 2) - 1));
            medianValue = (value1 + value2) / 2;
        } else {
            medianValue = allPurchaseValues.get((int) (allPurchaseValues.size() / 2));
        }

        context.write(new Text(key.getPurchaseCategory()), new FloatWritable(medianValue));
    }
}
