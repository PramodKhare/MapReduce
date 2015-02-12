package cs6240.assignments.a2.v4_new;
/**
 * @Purpose: MedianPurchaseReducer Reducer for finding median of purchases done in each category
 *           example.
 * @FileName MedianPurchaseReducerV3.java
 * @author Pramod Khare
 * @Created Sun 08 Feb 2015 23:22:21 PM EST
 * @Modified
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Reducer for MedianPurchase which reduces the key value pairs and computes the final value as
 * Category and Median
 */

public class MedianPurchaseReducerV4 extends
        Reducer<CategoryBinKey, BinCountValue, Text, IntWritable> {

    private static int BIN_SIZE = 1;

    /**
     * Called once at the beginning of the task.
     */
    @Override
    protected void setup(final Context context) throws IOException, InterruptedException {
        super.setup(context);
        configureParameters(context);
    }

    @Override
    public void reduce(CategoryBinKey key, Iterable<BinCountValue> values, Context context)
            throws IOException, InterruptedException {

        // Get all the values in the list in a temporary list
        final List<BinCountValue> binCountValuesList = new ArrayList<BinCountValue>();

        // Store total number of values count, which will used for median calculation
        int totalValuesCount = 0;
        for (BinCountValue value : values) {
            binCountValuesList.add(new BinCountValue(value.getBinNumber(), value.getCount()));
            totalValuesCount += value.getCount();
        }
        int medianValue = findApproxMedianFromBins(binCountValuesList, totalValuesCount);

        context.write(new Text(key.getPurchaseCategory()), new IntWritable(medianValue));
    }

    /**
     * Find the median value from total values count
     * 
     * @How - Find the median value index using total values count by 2, and iterate over all bins
     *      taking into consideration their individual counts till we reach the median value index,
     *      so that bin's value will be the approximate median
     * @param binCountValuesList    List of all BinCountValue objects
     * @param totalValuesCount      Total count of the prices in all bins
     *
     * @return approximate median value
     */
    private int findApproxMedianFromBins(final List<BinCountValue> binCountValuesList,
            final int totalValuesCount) {
        int medianValue;

        // Median of purchase-prices
        if ((totalValuesCount & 1) == 0) { // Even number of input lines or records
            medianValue = getMedianForEvenCount(binCountValuesList, totalValuesCount);

        } else { // Odd Number of lines or records
            medianValue = getMedianFromOddCount(binCountValuesList, totalValuesCount);
        }
        return medianValue;
    }

    /**
     * Calculates the median for odd number of input prices
     * @param binCountValuesList    List of all BinCountValue objects
     * @param totalValuesCount      Total count of the prices in all bins
     * @return median
     */
    private int getMedianFromOddCount(List<BinCountValue> binCountValuesList, int totalValuesCount) {

        int tempCount = 0;
        int medianValue = 0;

        int medianValueIndex = (totalValuesCount / 2) + 1;
        for (BinCountValue value : binCountValuesList) {
            if (tempCount >= medianValueIndex) {
                break;
            }
            tempCount += value.getCount();
            int lastBinNumber = value.getBinNumber();
            medianValue = BIN_SIZE * lastBinNumber;
        }
        return medianValue;
    }

    /**
     * Calculates the median for even number of input prices
     *
     * @param binCountValuesList    List of all BinCountValue objects
     * @param totalValuesCount      Total count of the prices in all bins
     * @return median
     */
    private int getMedianForEvenCount(List<BinCountValue> binCountValuesList, int totalValuesCount) {

        int medianValue = 0;
        int tempCount = 0;
        int lastBinNumber = 0;

        int medianValueIndex = totalValuesCount / 2;

        for (BinCountValue value : binCountValuesList) {
            if (tempCount == medianValueIndex) {

                // When two middle elements of median - is in two separate bins
                // Median value = average of both bin values
                medianValue = (BIN_SIZE * (lastBinNumber + value.getBinNumber())) / 2;
                break;
            } else if (tempCount > medianValueIndex) {

                // When two middle elements of median - are in same bin
                // Median value = that bin values itself
                medianValue = BIN_SIZE * lastBinNumber;
                break;
            }
            tempCount += value.getCount();
            lastBinNumber = value.getBinNumber();
        }
        return medianValue;
    }

    /**
     * Extract global parameters and set them
     * 
     * @param context context
     */
    private void configureParameters(
            Context context) {
        final Configuration conf = context.getConfiguration();
        BIN_SIZE = conf.getInt("BinSize", 1);
    }
}
