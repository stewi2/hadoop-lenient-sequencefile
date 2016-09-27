package stewi;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import stewi.mapred.LenientSequenceFileInputFormat;
import stewi.PairWritable;

public class MergeJob {

    public class MergeMapper extends Mapper<LongWritable, Text, MD5Hash, PairWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            MD5Hash md5 = new MD5Hash().digest(key.toString()).digest(value.toString());
            context.write(md5, new PairWritable(key,value));
        }
    }

    public class MergeReducer extends Reducer<MD5Hash, PairWritable, LongWritable, Text> {
        @Override
        protected void reduce(MD5Hash key, Iterable<PairWritable> values, Context context) throws IOException, InterruptedException {
            PairWritable pair = values.iterator().next();
            context.write(pair.getKey(), pair.getValue());
        }
    }

    public static void main( String[] args ) throws Exception
    {
            if(args.length !=2 ){
                    System.err.println("Usage : File Merge <input path> <output path>");
                    System.exit(-1);
            }
            Configuration conf = new Configuration();
            Job job = new Job(conf);
            job.setJarByClass(MergeJob.class);
            job.setJobName("Merge "+args[0]+" "+args[1]);
             
            LenientSequenceFileInputFormat.addInputPath(job,new Path(args[0]) );
            LenientSequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));

            job.setMapperClass(MergeMapper.class);
            job.setCombinerClass(MergeReducer.class); 
            job.setReducerClass(MergeReducer.class);
             
            job.setInputFormatClass(LenientSequenceFileInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
             
            job.setOutputKeyClass(LongWritable.class);
            iob.setOutputValueClass(Text.class);
            job.setNumReduceTasks(4);
            
            System.exit(job.waitForCompletion(true) ? 0:1);
    }

    
}
