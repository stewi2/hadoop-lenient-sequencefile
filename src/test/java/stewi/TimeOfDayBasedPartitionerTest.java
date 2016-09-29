package stewi;

import static org.junit.Assert.*;

import java.util.Calendar;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.Partitioner;
import org.junit.Test;

public class TimeOfDayBasedPartitionerTest {

    @Test
    public void testGetPartition() {
        Partitioner<LongWritable,NullWritable> partitioner = new TimeOfDayBasedPartitioner<>();
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.SECOND, 30);
        cal.set(Calendar.MINUTE, 30);
        cal.set(Calendar.HOUR_OF_DAY, 1);
        assertEquals(0, partitioner.getPartition(new LongWritable(cal.getTimeInMillis()), NullWritable.get(), 2));
        cal.set(Calendar.HOUR_OF_DAY, 11);
        assertEquals(0, partitioner.getPartition(new LongWritable(cal.getTimeInMillis()), NullWritable.get(), 2));
        cal.set(Calendar.HOUR_OF_DAY, 12);
        assertEquals(1, partitioner.getPartition(new LongWritable(cal.getTimeInMillis()), NullWritable.get(), 2));
        cal.set(Calendar.HOUR_OF_DAY, 23);
        assertEquals(1, partitioner.getPartition(new LongWritable(cal.getTimeInMillis()), NullWritable.get(), 2));
        cal.set(2016, 4, 5, 23, 59, 59);
        assertEquals(1, partitioner.getPartition(new LongWritable(cal.getTimeInMillis()), NullWritable.get(), 2));
        cal.set(2016, 4, 5, 00, 50, 00);
        assertEquals(1, partitioner.getPartition(new LongWritable(cal.getTimeInMillis()), NullWritable.get(), 40));
    }

}
