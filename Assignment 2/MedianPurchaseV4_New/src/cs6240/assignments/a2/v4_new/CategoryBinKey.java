package cs6240.assignments.a2.v4_new;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.MessageFormat;

/**
 * @Class_Name: CategoryBinKey.java (Combination Key class for Purchase Category and Bin Number in
 *              which its price belongs)
 * @Purpose: This is a Composite key class. The natural key is the Purchase Category value. The
 *           secondary sort will be performed against the bin-number.
 * @author Pramod Khare Srikar Reddy Demagu
 * @Created Sun 08 Feb 2015 23:22:21 PM EST
 * @Modified Sun 10 Feb 2015 23:12:11 PM EST
 */
public class CategoryBinKey implements WritableComparable<CategoryBinKey> {

    private String purchaseCategory;
    private int binNumber;

    public CategoryBinKey() {}

    public CategoryBinKey(final String purchaseCategory, final int binNumber) {
        this.purchaseCategory = purchaseCategory;
        this.binNumber = binNumber;
    }

    @Override
    public String toString() {
        return MessageFormat.format("'{'{0},{1}'}'", this.purchaseCategory, this.binNumber);
    }

    @Override
    public void readFields(final DataInput in) throws IOException {
        this.purchaseCategory = WritableUtils.readString(in);
        this.binNumber = in.readInt();
    }

    @Override
    public void write(final DataOutput out) throws IOException {
        WritableUtils.writeString(out, this.purchaseCategory);
        out.writeInt(this.binNumber);
    }

    @Override
    public int compareTo(final CategoryBinKey key2) {
        int result = this.purchaseCategory.compareTo(key2.purchaseCategory);
        if (0 == result) {
            result =
                    (this.binNumber < key2.binNumber) ? -1
                            : ((this.binNumber == key2.binNumber) ? 0 : 1);
        }
        return result;
    }

    /********************************************************
     * Getters and Setters
     ********************************************************/
    public String getPurchaseCategory() {
        return purchaseCategory;
    }

    public void setPurchaseCategory(final String purchaseCategory) {
        this.purchaseCategory = purchaseCategory;
    }

    public int getBinNumber() {
        return binNumber;
    }

    public void setBinNumber(final int binNumber) {
        this.binNumber = binNumber;
    }
}
