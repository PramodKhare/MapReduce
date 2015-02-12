package cs6240.assignments.a2.v4_new;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.MessageFormat;

/**
 * @Class_Name: BinCountValue.java (Combination value class for mapper function)
 * @Purpose: This is a Composite value class. The natural part is the Bin Number value, and
 *           remaining is value-counts in that bin.
 * @author Pramod Khare, Srikar Reddy D
 * @Created Sun 08 Feb 2015 23:22:21 PM EST
 * @Modified Sun 10 Feb 2015 22:04:19 PM EST
 */

public class BinCountValue implements WritableComparable<BinCountValue> {

    private int binNumber = 0;
    private int count = 0;

    public BinCountValue() {}

    public BinCountValue(final int binNumber, final int count) {
        this.binNumber = binNumber;
        this.count = count;
    }

    @Override
    public String toString() {
        return MessageFormat.format("'{'{0},{1}'}'", this.binNumber, this.count);
    }

    @Override
    public void readFields(final DataInput in) throws IOException {
        this.binNumber = in.readInt();
        this.count = in.readInt();
    }

    @Override
    public void write(final DataOutput out) throws IOException {
        out.writeInt(this.binNumber);
        out.writeInt(this.count);
    }

    @Override
    public int compareTo(final BinCountValue key2) {
        int result =
                (this.binNumber < key2.binNumber) ? -1 : ((this.binNumber == key2.binNumber) ? 0
                        : 1);
        if (0 == result) {
            result = (this.count < key2.count) ? -1 : ((this.count == key2.count) ? 0 : 1);
        }
        return result;
    }

    /********************************************************
     * Getters and Setters
     ********************************************************/

    public int getCount() {
        return count;
    }

    public void setCount(final int count) {
        this.count = count;
    }

    public int getBinNumber() {
        return binNumber;
    }

    public void setBinNumber(final int binNumber) {
        this.binNumber = binNumber;
    }
}
