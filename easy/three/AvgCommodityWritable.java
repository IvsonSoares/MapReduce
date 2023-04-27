package tde_grupo.easy.three;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AvgCommodityWritable implements WritableComparable<AvgCommodityWritable> {
    private float somaQuantidade;
    private int n;

    public AvgCommodityWritable() {}

    public AvgCommodityWritable(float somaQuantidade, int n) {
        this.somaQuantidade = somaQuantidade;
        this.n = n;
    }

    public float getSomaQuantidade() {
        return somaQuantidade;
    }

    public void setSomaQuantidade(float somaQuantidade) {
        this.somaQuantidade = somaQuantidade;
    }

    public int getN() {
        return n;
    }

    public void setN(int n) {
        this.n = n;
    }

    @Override
    public int compareTo(AvgCommodityWritable o) {
        return Integer.compare(this.hashCode(), o.hashCode());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeFloat(somaQuantidade);
        dataOutput.writeInt(n);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        somaQuantidade = dataInput.readFloat();
        n         = dataInput.readInt();
    }
}
