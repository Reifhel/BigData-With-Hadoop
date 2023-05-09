package TDE_Codigos.x6.Writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.io.WritableComparable;

public class AverageFlowWritable 
    implements WritableComparable<AverageFlowWritable> {

    private Double average;
    private int count;

    public AverageFlowWritable() {
        this.average = 0.0;
        this.count = 0;
    }

    public AverageFlowWritable(Double average, int count) {
        this.average = average;
        this.count = count;
    }

    public Double getAverage() {
        return average;
    }

    public void setAverage(Double average) {
        this.average = average;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(average);
        out.writeInt(count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        average = in.readDouble();
        count = in.readInt();
    }

    @Override
    public int compareTo(AverageFlowWritable o) {
        return Double.compare(average, o.average);
    }

    @Override
    public int hashCode() {
        return Objects.hash(average, count);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        AverageFlowWritable other = (AverageFlowWritable) obj;
        return Double.doubleToLongBits(average) == Double.doubleToLongBits(other.average) && count == other.count;
    }

    @Override
    public String toString() {
        return Double.toString(average);
    }
    
}
