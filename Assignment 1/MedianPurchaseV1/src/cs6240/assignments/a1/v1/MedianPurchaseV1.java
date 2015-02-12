package cs6240.assignments.a1.v1;

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
 * @author Pramod Khare
 * @Created Mon 27 Jan 2015 11:45:24 PM EST
 * @Modified Tue 28 Jan 2015 10:45:24 AM EST
 */

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class MedianPurchaseV1 {

    public final static Pattern pattern = Pattern.compile("\\t+");
    private final static Map<String, List<Float>> purchasePricesByCategory =
            new HashMap<String, List<Float>>();

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MedianPurchaseV1 <input path> <output path>");
            System.exit(-1);
        }

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
                List<Float> prices = purchasePricesByCategory.get(category);
                Collections.sort(prices);

                float medianValue;
                // Median of purchase-prices
                // If even or odd number of purchases
                if ((prices.size() & 1) == 0) {
                    float value1 = prices.get((int) (prices.size() / 2));
                    float value2 = prices.get((int) ((prices.size() / 2) - 1));
                    medianValue = (value1 + value2) / 2;
                } else {
                    medianValue = prices.get((int) (prices.size() / 2));
                }
                // Append to output file
                bw.write(category + "\t" + medianValue + "\n");
            }

            bw.flush();
            bw.close();
        } catch (final IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
            } catch (final IOException ex) {
                ex.printStackTrace();
            }
        }
    }

    public static void processPurchaseEntries(final String line) {
        // Split the line along the tab characters
        try {
            String[] tokens = pattern.split(line);
            final String category = tokens[3];
            final Float price = Float.parseFloat(tokens[4]);

            // check if category exists in the map
            List<Float> prices = purchasePricesByCategory.get(category);
            if (prices != null) {
                prices.add(price);
            } else {
                prices = new ArrayList<Float>();
                prices.add(price);
                purchasePricesByCategory.put(category, prices);
            }
        } catch (final Exception e) {
            // ignore the invalid inputs
        }
    }
}
