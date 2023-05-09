package TDE_Codigos.x4.Writables;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.checkerframework.checker.units.qual.Temperature;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class PriceAvgWritable 
            implements WritableComparable<PriceAvgWritable>{
    private double price;
    private int count;

    public PriceAvgWritable() {
        this.price = 0;
        this.count = 0;
    }

    public PriceAvgWritable(double price, int count) {
        this.price = price;
        this.count = count;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public int compareTo(PriceAvgWritable o) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'compareTo'");
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        // escrevendo em uma ordem desejada
        dataOutput.writeDouble(price);
        dataOutput.writeInt(count);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        // lendo na mesma ordem com que os dados foram escritos anteriormente
        price = dataInput.readDouble();
        count = dataInput.readInt();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PriceAvgWritable that = (PriceAvgWritable) o;
        return Double.compare(that.price, price) == 0 && count == that.count;
    }

    @Override
    public int hashCode() {
        return Objects.hash(price, count);
    }


}
