package tde_grupo.easy.two;

import advanced.customwritable.ForestFireWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class TransactionsFlowtypeYearWritable implements WritableComparable<TransactionsFlowtypeYearWritable> {

    private String flowtype;
    private String year;

    public String getFlowtype() {
        return flowtype;
    }

    public String getYear() {
        return year;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TransactionsFlowtypeYearWritable that = (TransactionsFlowtypeYearWritable) o;
        return Objects.equals(flowtype, that.flowtype) && Objects.equals(year, that.year);
    }

    @Override
    public int hashCode() {
        return Objects.hash(flowtype, year);
    }

    public void setYear(String year) {
        this.year = year;
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(flowtype);
        dataOutput.writeUTF(year);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        flowtype = dataInput.readUTF();
        year = dataInput.readUTF();
    }

    public TransactionsFlowtypeYearWritable(String flowtype, String year) {
        this.flowtype = flowtype;
        this.year = year;
    }
    public TransactionsFlowtypeYearWritable() {}


    @Override
    public int compareTo(TransactionsFlowtypeYearWritable o) {
        return Integer.compare(this.hashCode(), o.hashCode());
    }

    @Override
    public String toString() {
        return "TransactionsFlowtypeYearWritable{" +
                "flowtype='" + flowtype + '\'' +
                ", year='" + year + '\'' +
                '}';
    }
}
