package mapreduce;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class MobileDataUsage implements Writable {


    private long upstreamTraffic;  // 上行流量
    private long downstreamTraffic;  // 下行流量
    private long totalTraffic;   // 总流量

    // 反序列化时,需要反射调用空参构造函数,所以要显式定义一个
    public MobileDataUsage() {}

    public MobileDataUsage(long upFlow, long downFlow) {
        this.upstreamTraffic = upFlow;
        this.downstreamTraffic = downFlow;
        this.totalTraffic = upFlow + downFlow;
    }

    public long getUpstreamTraffic() {
        return upstreamTraffic;
    }

    public void setUpstreamTraffic(long upstreamTraffic) {
        this.upstreamTraffic = upstreamTraffic;
    }

    public long getDownstreamTraffic() {
        return downstreamTraffic;
    }

    public void setDownstreamTraffic(long downstreamTraffic) {
        this.downstreamTraffic = downstreamTraffic;
    }

    public long getTotalTraffic() {
        return totalTraffic;
    }

    public void setTotalTraffic(long totalTraffic) {
        this.totalTraffic = totalTraffic;
    }

    /**
     * 序列化方法
     */
    public void write(DataOutput out) throws IOException {
        out.writeLong(upstreamTraffic);
        out.writeLong(downstreamTraffic);
        out.writeLong(totalTraffic);
    }

    /**
     * 反序列化方法:
     * 注意 : 反序列化的顺序跟序列化的顺序完全一致
     */
    public void readFields(DataInput in) throws IOException {
        upstreamTraffic = in.readLong();
        downstreamTraffic = in.readLong();
        totalTraffic = in.readLong();
    }

    // 输出打印的时候调用的是toString() 方法
    @Override
    public String toString() {
        return upstreamTraffic + "\t" + downstreamTraffic + "\t" + totalTraffic;
    }
}

