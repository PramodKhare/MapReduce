package cs6240.assignments.a2.v4_new;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @Class_Name: CompositeKeyComparator.java
 * @Purpose: Custom comparator class for comparing composite keys.
 * @author Pramod Khare Srikar Reddy Demagu
 * @Created Sun 08 Feb 2015 23:22:21 PM EST
 * @Modified Sun 10 Feb 2015 20:22:21 PM EST
 */

@SuppressWarnings("rawtypes")
public class CompositeKeyComparator extends WritableComparator {
    protected CompositeKeyComparator() {
        super(CategoryBinKey.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        CategoryBinKey k1 = (CategoryBinKey) w1;
        CategoryBinKey k2 = (CategoryBinKey) w2;

        int result = k1.getPurchaseCategory().compareTo(k2.getPurchaseCategory());
        if (0 == result) {
            result =
                    (k1.getBinNumber() < k2.getBinNumber()) ? -1 : ((k1.getBinNumber() == k2
                            .getBinNumber()) ? 0 : 1);
        }
        return result;
    }
}
