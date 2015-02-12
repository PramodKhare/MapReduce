package cs6240.assignments.a2.v3;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @Class_Name: CompositeKeyComparator.java
 * @Purpose: Custom comparator class for comparing composite keys.
 * @author Pramod Khare
 * @Created Sun 08 Feb 2015 22:59:24 PM EST
 * @Modified
 */

@SuppressWarnings("rawtypes")
public class CompositeKeyComparator extends WritableComparator {
    protected CompositeKeyComparator() {
        super(PurchaseCategoryPriceCombinationKey.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        PurchaseCategoryPriceCombinationKey k1 = (PurchaseCategoryPriceCombinationKey) w1;
        PurchaseCategoryPriceCombinationKey k2 = (PurchaseCategoryPriceCombinationKey) w2;

        int result = k1.getPurchaseCategory().compareTo(k2.getPurchaseCategory());
        if (0 == result) {

            result =
                    (k1.getPurchasePrice() < k2.getPurchasePrice()) ? -1
                            : ((k1.getPurchasePrice() == k2.getPurchasePrice()) ? 0 : 1);
        }
        return result;
    }
}
