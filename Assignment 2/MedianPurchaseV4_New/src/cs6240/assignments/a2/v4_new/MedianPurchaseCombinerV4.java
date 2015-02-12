package cs6240.assignments.a2.v4_new;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Purpose: MedianPurchaseCombinerV4 - Combiner class combines the bin-counts for same-bin-numbers
 *           and same-category-value
 * @FileName MedianPurchaseCombinerV4.java
 * INPUT                                        OUTPUT
 *     (baby, 20) (20, 1)                       (baby, 20) (20, 2)
 * Ex: (baby, 20) (20, 1)                       (baby, 30) (30, 1)
 *     (baby, 30) (30, 1)           ---->       (boy, 40) (40, 2)
 *     (boy, 40)  (40, 1)
 *     (boy, 40)  (40, 1)
 *
 * @author Pramod Khare, Srikar Reddy D
 * @Created Sun 08 Feb 2015 23:22:21 PM EST
 * @Modified
 */

public class MedianPurchaseCombinerV4 extends
        Reducer<CategoryBinKey, BinCountValue, CategoryBinKey, BinCountValue> {

    @Override
    public void reduce(CategoryBinKey key, Iterable<BinCountValue> values, Context context)
            throws IOException, InterruptedException {

        int currentBinNumber, currentBinCount;

        // Merge all the counts for same category and same bin-number keys,
        // and emit them
        BinCountValue mergedBinCount = null;
        for (BinCountValue value : values) {

            currentBinNumber = value.getBinNumber();
            currentBinCount = value.getCount();

            // This is for first element in the list
            if (mergedBinCount == null) {
                mergedBinCount = new BinCountValue(currentBinNumber, currentBinCount);
                continue;
            }

            // compares bin number with the previous element and increment bin count
            if (mergedBinCount.getBinNumber() == currentBinNumber) {
                mergedBinCount.setCount(mergedBinCount.getCount() + currentBinCount);
            }
            // writes to context if there is a different bin number
            else {
                context.write(key, mergedBinCount);
                mergedBinCount = new BinCountValue(currentBinNumber, currentBinCount);
            }
        }

        // This is for last remaining mergedBinCount element
        context.write(key, mergedBinCount);
    }
}
