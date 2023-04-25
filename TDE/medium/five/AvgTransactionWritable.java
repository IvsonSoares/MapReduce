package TDE.medium.five;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class AvgTransactionWritable implements WritableComparable<AvgTransactionWritable> {
    private float somaPrice;

    private int n;

    public AvgTransactionWritable() {}

    public AvgTransactionWritable(float somaPrice, int n) {
        this.somaPrice = somaPrice;
        this.n = n;
    }

    public float getSomaPrice() {
        return somaPrice;
    }

    public void setSomaPrice(float somaPrice) {
        this.somaPrice = somaPrice;
    }

    public int getN() {
        return n;
    }

    public void setN(int n) {
        this.n = n;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AvgTransactionWritable that = (AvgTransactionWritable) o;
        return Float.compare(that.somaPrice, somaPrice) == 0 && n == that.n;
    }

    @Override
    public int hashCode() {
        return Objects.hash(somaPrice, n);
    }


    @Override
    public int compareTo(AvgTransactionWritable o) {
        return Integer.compare(this.hashCode(), o.hashCode());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeFloat(somaPrice);
        dataOutput.writeInt(n);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        somaPrice = dataInput.readFloat();
        n         = dataInput.readInt();
    }
}
