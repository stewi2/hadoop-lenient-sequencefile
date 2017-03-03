package stewi;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import java.io.OutputStreamWriter;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class WriteFile {

    public static void main(String[] args) throws Exception {
        // Logger.getRootLogger().setLevel(Level.DEBUG);

        Configuration conf = new Configuration();

	CompressionType type = CompressionType.valueOf(args[1]);
        Path path = new Path(URI.create(args[0]));
        FSDataOutputStream out = path.getFileSystem(conf).create(path);

        SequenceFile.Writer writer = SequenceFile.createWriter(conf,
                Writer.stream(out), Writer.keyClass(LongWritable.class),
                Writer.valueClass(Text.class),
                Writer.compression(type, new SnappyCodec()));

        for(int i=0; i<12345; i++) {
            writer.append(new LongWritable(i),
                    new Text("#>"+RandomStringUtils.randomAlphanumeric(100)+"<#"));
//            if(i%1000==0) {
//                System.out.println("Sync after "+i);
//                writer.sync();
//                out.flush();
//                out.sync();
//            }
        }

//        Thread.sleep(3000);
//        Runtime.getRuntime().halt(0);
//        writer.hflush();
        writer.close();
        out.close();
    }

}
