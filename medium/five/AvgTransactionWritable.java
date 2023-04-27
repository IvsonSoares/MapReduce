package tde_grupo.medium.five;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class AvgTransactionWritable implements WritableComparable<AvgTransactionWritable> {
    private double somaPrice;

    private int n;
    private double min;
    private double max;

    public AvgTransactionWritable() {}

    public AvgTransactionWritable(double somaPrice, int n, double max, double min) {
        this.somaPrice = somaPrice;
        this.n = n;
        this.max = max;
        this.min = min;
    }

    public double getSomaPrice() {
        return somaPrice;
    }

    public void setSomaPrice(double somaPrice) {
        this.somaPrice = somaPrice;
    }

    public int getN() {
        return n;
    }

    public void setN(int n) {
        this.n = n;
    }

    public double getMin() {
        return min;
    }

    public void setMin(double min) {
        this.min = min;
    }

    public double getMax() {
        return max;
    }

    public void setMax(double max) {
        this.max = max;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AvgTransactionWritable that = (AvgTransactionWritable) o;
        return Double.compare(that.somaPrice, somaPrice) == 0 && n == that.n && Double.compare(that.max, max) ==0 &&Double.compare(that.min, min)==0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(somaPrice, n, max, min);
    }


    @Override
    public int compareTo(AvgTransactionWritable o) {
        return Integer.compare(this.hashCode(), o.hashCode());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(somaPrice);
        dataOutput.writeInt(n);
        dataOutput.writeDouble(max);
        dataOutput.writeDouble(min);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        somaPrice   = dataInput.readDouble();
        n           = dataInput.readInt();
        max         = dataInput.readDouble();
        min         = dataInput.readDouble();
    }
}
