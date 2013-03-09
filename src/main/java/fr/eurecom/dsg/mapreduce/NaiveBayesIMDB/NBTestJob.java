package fr.eurecom.dsg.mapreduce.NaiveBayesIMDB;
import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class NBTestJob extends Job {

	public NBTestJob(Configuration conf
							, Path positivePath
							, Path negativePath
							, Path trainOutputPath
							, Path outputPath) throws IOException, URISyntaxException {
		super(conf);
		
		this.setJarByClass(NBTestJob.class);
		
		this.setMapOutputKeyClass(Text.class);
		this.setMapOutputValueClass(BooleanWritable.class);
		
		this.setReducerClass(NBTestReducer.class);
		
		this.setOutputKeyClass(Text.class);
		this.setOutputValueClass(DoubleWritable.class);
		this.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(this, outputPath);

		MultipleInputs.addInputPath(this
															, positivePath
															, TextInputFormat.class
															, NBTestPositiveMapper.class);
		
		
		MultipleInputs.addInputPath(this
															, negativePath
															, TextInputFormat.class
															, NBTestNegativeMapper.class);
		
    FileSystem fs = FileSystem.get(conf);
    
		for (FileStatus status : fs.listStatus(trainOutputPath)) {
		  if (status.getPath().getName().startsWith("part-")) {
		    System.out.println("Caching file " + status.getPath().toUri());
			  DistributedCache.addCacheFile(status.getPath().toUri(),conf);
		  }
		}
		
		fs.close();
	}
}
