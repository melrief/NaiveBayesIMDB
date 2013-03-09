package fr.eurecom.dsg.mapreduce.NaiveBayesIMDB;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import fr.eurecom.dsg.mapreduce.NaiveBayesIMDB.utils.ClassOccurrences;


public class NBTrainJob extends Job {

	public final static Text POS = new Text("POS");
	public final static Text NEG = new Text("NEG");
	public final static Text CLASS_KEY = new Text(" ");
	public static final String WORD_CLASS_SEP = "_";

	public NBTrainJob(Configuration conf
							 , Path positivePath
							 , Path negativePath
							 , Path outputPath) throws IOException {
		this(conf, positivePath, negativePath, null, outputPath);
	}
	
	
	public NBTrainJob(Configuration conf
							 , Path positivePath
							 , Path negativePath
							 , URI excludedWordsURI
							 , Path outputPath) throws IOException {
		super(conf, "Naive Bayes Train");
		
		this.setJarByClass(NBTrainJob.class);
		
		this.setMapOutputKeyClass(Text.class);
		this.setMapOutputValueClass(ClassOccurrences.class);
		
		MultipleInputs.addInputPath(this
															, positivePath
															, TextInputFormat.class
															, NBPositiveMapper.class);
		
		MultipleInputs.addInputPath(this
															, negativePath
															, TextInputFormat.class
															, NBNegativeMapper.class);
		
		
		this.setCombinerClass(NBTrainCombiner.class);
		
		this.setReducerClass(NBTrainReducer.class);
		this.setOutputKeyClass(Text.class);
		this.setOutputValueClass(LongWritable.class);
		this.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(this, outputPath);

		if (excludedWordsURI != null) {
		  FileSystem fs = FileSystem.get(this.getConfiguration());
		  if (!fs.exists(new Path(excludedWordsURI.getPath()))) {
		    throw new FileNotFoundException(excludedWordsURI.getPath());
		  }
		  fs.close();
			System.out.println("Caching file " + excludedWordsURI.toString());
			DistributedCache.addCacheFile(excludedWordsURI, this.getConfiguration());
		}
	}

}