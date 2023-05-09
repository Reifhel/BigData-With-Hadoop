package TDE_Codigos.x7.Writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.io.WritableComparable;

public class FlowCommodityWritable 
    implements WritableComparable<FlowCommodityWritable>{

    private String commodity;
    private String flow;

    public FlowCommodityWritable() {
    }

    public FlowCommodityWritable(String commodity, String flow) {
        this.commodity = commodity;
        this.flow = flow;
    }

    public String getCommodity() {
        return commodity;
    }

    public void setCommodity(String commodity) {
        this.commodity = commodity;
    }

    public String getFlow() {
        return flow;
    }

    public void setFlow(String flow) {
        this.flow = flow;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(commodity);
        out.writeUTF(flow);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        commodity = in.readUTF();
        flow = in.readUTF();
    }

    @Override
    public int compareTo(FlowCommodityWritable o) {
        return o.hashCode() - this.hashCode();
    }

    @Override
    public int hashCode() {
        return Objects.hash(commodity, flow);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        FlowCommodityWritable other = (FlowCommodityWritable) obj;
        return Objects.equals(commodity, other.commodity) && Objects.equals(flow, other.flow);
    }

    @Override
    public String toString() {
        return String.format("%s ; %s ;", flow, commodity);
    }
    
}
