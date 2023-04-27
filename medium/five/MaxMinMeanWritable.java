package tde_grupo.medium.five;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class MaxMinMeanWritable implements WritableComparable<MaxMinMeanWritable> {
    private double max;
    private double min;

    private double mean;

    public MaxMinMeanWritable() {}

    public MaxMinMeanWritable(double max, double min, double mean) {
        this.max = max;
        this.min = min;
        this.mean = mean;
    }

    public double getMax() {
        return max;
    }

    public void setMax(double max) {
        this.max = max;
    }

    public double getMin() {
        return min;
    }

    public void setMin(double min) {
        this.min = min;
    }

    public double getMean() {
        return mean;
    }

    public void setMean(double mean) {
        this.mean = mean;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MaxMinMeanWritable that = (MaxMinMeanWritable) o;
        return Double.compare(that.max, max) == 0 && Double.compare(that.min, min) == 0 && Double.compare(that.mean, mean) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(max, min, mean);
    }

    @Override
    public int compareTo(MaxMinMeanWritable o) {
        return Integer.compare(this.hashCode(), o.hashCode());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(max);
        dataOutput.writeDouble(min);
        dataOutput.writeDouble(mean);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        max  = dataInput.readDouble();
        min  = dataInput.readDouble();
        mean = dataInput.readDouble();
    }

    @Override
    public String toString() {
        return
                "max=" + max +
                ", min=" + min +
                ", mean=" + mean;
    }
}
