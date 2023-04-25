package TDE.medium.five;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class MaxMinMeanWritable implements WritableComparable<MaxMinMeanWritable> {
    private float max;
    private float min;

    private float mean;

    public MaxMinMeanWritable() {}

    public MaxMinMeanWritable(float max, float min, float mean) {
        this.max = max;
        this.min = min;
        this.mean = mean;
    }

    public float getMax() {
        return max;
    }

    public void setMax(float max) {
        this.max = max;
    }

    public float getMin() {
        return min;
    }

    public void setMin(float min) {
        this.min = min;
    }

    public float getMean() {
        return mean;
    }

    public void setMean(float mean) {
        this.mean = mean;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MaxMinMeanWritable that = (MaxMinMeanWritable) o;
        return Float.compare(that.max, max) == 0 && Float.compare(that.min, min) == 0 && Float.compare(that.mean, mean) == 0;
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
        dataOutput.writeFloat(max);
        dataOutput.writeFloat(min);
        dataOutput.writeFloat(mean);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        max  = dataInput.readFloat();
        min  = dataInput.readFloat();
        mean = dataInput.readFloat();
    }

    @Override
    public String toString() {
        return "MaxMinMeanWritable{" +
                "max=" + max +
                ", min=" + min +
                ", mean=" + mean +
                '}';
    }
}
