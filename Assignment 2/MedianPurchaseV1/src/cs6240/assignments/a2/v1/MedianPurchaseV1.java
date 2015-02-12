package cs6240.assignments.a2.v1;

/**
 * @Problem_Desc: Input to program should accept a file with tab separated columns. The fourth
 *                column contains free form text describing an item category (for instance,
 *                "Children's toys") and the fifth a price in dollars and cents (for instance,
 *                12.33).
 * 
 *                Your program should compute the median of purchases by item category, and it
 *                should be robust to malformed inputs.
 * 
 * @FileName MedianPurchaseV1.java (Java sequential File Processing Implementation)
 * @author Pramod Khare, Srikar Reddy Demagu
 * @Created Sun 08 Feb 2015 22:59:24 PM EST
 * @Modified
 */

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class MedianPurchaseV1 {

    public static final String TAB_CHAR = "\t";
    public static final String NEWLINE = "\n";
    public final static Pattern pattern = Pattern.compile("\\t+");
    private final static Map<String, List<Integer>> purchasePricesByCategory =
            new HashMap<String, List<Integer>>();

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MedianPurchaseV1 <input path> <output path>");
            System.exit(-1);
        }

        long startTime = System.currentTimeMillis();
        final File inputFile = new File(args[0]);
        final File outputFile = new File(args[1]);
        if (!inputFile.exists()) {
            System.err.println("Input file doesn't exists");
            System.exit(-1);
        }

        FileWriter fw = null;
        BufferedReader br = null;
        BufferedWriter bw = null;

        // If output.txt file exists, then do not overwrite the results, stop!!
        if (outputFile.exists()) {
            System.err.println("Output file already exists. Please delete the output file first.");
            System.exit(-1);
        } else {
            // if file does not exists, then create it
            outputFile.createNewFile();
            fw = new FileWriter(outputFile.getAbsoluteFile());
            bw = new BufferedWriter(fw);
        }

        try {
            // FileInputStream fis = new FileInputStream(inputFile);
            // br = new BufferedReader(new InputStreamReader(fis));
            br = new BufferedReader(new FileReader(inputFile));

            String line = null;
            // Skip first line - it only contains headers
            if ((line = br.readLine()) == null) {
                System.err.println("Input file is empty, please provide valid input file");
                System.exit(-1);
            }

            // Read input file line by line and find & sort all purchases
            // for every particular category to find median
            while ((line = br.readLine()) != null) {
                processPurchaseEntries(line);
            }

            // For each key, sort the prices and take the median price
            for (final String category : purchasePricesByCategory.keySet()) {
                List<Integer> prices = purchasePricesByCategory.get(category);
                Collections.sort(prices);

                float medianValue = findMedianOfSortedList(prices);

                // Append to output file
                bw.write(category + TAB_CHAR + medianValue + NEWLINE);
            }
            long endTime = System.currentTimeMillis();
            System.out.println("Total time - " + (endTime - startTime));
        } catch (final IOException e) {
            System.err.println("Failed to calculate median value of input - " + e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
                if (bw != null) {
                    bw.flush();
                    bw.close();
                }
                if (fw != null) {
                    fw.close();
                }
            } catch (final IOException ex) {
                System.err.println("Unable to close file-streams");
                ex.printStackTrace();
            }
        }
    }

    /**
     * Parse the given input line - (i.e. single purchase entry) and store purchase category and its
     * price into Global HashMap
     * 
     * @param line
     */
    public static void processPurchaseEntries(final String line) {
        // Split the line along the tab characters
        try {
            String[] tokens = pattern.split(line);
            // check if category exists in the map
            // Instead of using containsKey() and then get() from HashMap -
            // single get() call will return either null or contained value.
            List<Integer> prices = purchasePricesByCategory.get(tokens[3]);
            if (prices != null) {
                prices.add((int) Float.parseFloat(tokens[4]));
            } else {
                purchasePricesByCategory.put(tokens[3],
                        new ArrayList<Integer>(Arrays.asList((int) Float.parseFloat(tokens[4]))));
            }
        } catch (final Exception e) {
            // ignore the invalid inputs
        }
    }

    /**
     * Returns the Median value of given sorted list of values
     * 
     * @param prices
     * @return median value
     */
    public static int findMedianOfSortedList(List<Integer> prices) {
        // Median of purchase-prices
        // If even or odd number of purchases
        if ((prices.size() & 1) == 0) {
            int value1 = prices.get((int) (prices.size() / 2));
            int value2 = prices.get((int) ((prices.size() / 2) - 1));
            return (value1 + value2) / 2;
        } else {
            return prices.get((int) (prices.size() / 2));
        }
    }
}
