package TDE_Codigos.x5.Writables;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.checkerframework.checker.units.qual.Temperature;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class MinMaxAvgWritable 
            implements WritableComparable<MinMaxAvgWritable>{
	double max;
	double min;
	double mean;


    public MinMaxAvgWritable(double max, double min, double mean) {
		this.max = max;
		this.min = min;
		this.mean = mean;
	}
	
	

	public MinMaxAvgWritable() {
		super();
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
    public int compareTo(MinMaxAvgWritable o) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'compareTo'");
    }
	
	

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        // escrevendo em uma ordem desejada
        dataOutput.writeDouble(max);
        dataOutput.writeDouble(min);
        dataOutput.writeDouble(mean);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        // lendo na mesma ordem com que os dados foram escritos anteriormente
    	max = dataInput.readDouble();
    	min = dataInput.readDouble();
    	mean = dataInput.readDouble();
    }

    @Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		MinMaxAvgWritable other = (MinMaxAvgWritable) obj;
		return Double.doubleToLongBits(max) == Double.doubleToLongBits(other.max)
				&& Double.doubleToLongBits(mean) == Double.doubleToLongBits(other.mean)
				&& Double.doubleToLongBits(min) == Double.doubleToLongBits(other.min);
	}
    
    

    @Override
	public int hashCode() {
		return Objects.hash(max, mean, min);
	}



	@Override
	public String toString() {
		return "[max=" + max + ", min=" + min + ", mean=" + mean + "]";
	}
    
    


}
