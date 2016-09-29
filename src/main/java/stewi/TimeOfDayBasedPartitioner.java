package stewi;

import java.util.Calendar;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Partitioner;

public class TimeOfDayBasedPartitioner<V> extends MapReduceBase implements Partitioner<LongWritable, V> {
    @Override
    public int getPartition(LongWritable key, V value, int numPartitions) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(key.get());
        int second = cal.get(Calendar.HOUR_OF_DAY) * 3600 + 60 * cal.get(Calendar.MINUTE) + cal.get(Calendar.SECOND);
        return (int)(Math.floor((float)second/86400 * numPartitions));  
    }
}