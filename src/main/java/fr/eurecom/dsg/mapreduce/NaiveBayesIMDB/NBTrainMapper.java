package fr.eurecom.dsg.mapreduce.NaiveBayesIMDB;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import fr.eurecom.dsg.mapreduce.NaiveBayesIMDB.utils.ClassOccurrences;



public abstract class NBTrainMapper 
						extends Mapper<LongWritable, Text, Text, ClassOccurrences> {

	private HashSet<String> excludedWords;
	
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		this.excludedWords = new HashSet<String>();
		Path[] cacheFiles = DistributedCache.getLocalCacheFiles(
																									context.getConfiguration());
		if (cacheFiles != null) {
			System.out.println("Excluding words in " + Arrays.toString(cacheFiles));
			for (Path cacheFile : cacheFiles) {
				BufferedReader reader = new BufferedReader(
																					new FileReader(cacheFile.toString()));
				String excludedWord;
				while ((excludedWord = reader.readLine()) != null) {
					excludedWords.add(excludedWord);
				}
				
				reader.close();
			}
		} else {
			System.out.println("No file cached");
		}
	}
	
	protected HashSet<String> getExcludedWords() {
		return this.excludedWords;
	}
	
	public abstract Text getReviewType();
	
	public ClassOccurrences getClassWithOne() {
		return new ClassOccurrences(this.getReviewType(), 1l);
	}
	
	@Override
	protected void map(LongWritable offset, Text line, Context context) 
			throws IOException, InterruptedException {
		context.write(NBTrainJob.CLASS_KEY, this.getClassWithOne());
		String sline = line.toString().trim().toLowerCase();
		for (String word : sline.replaceAll("[^a-zA-Z0-9'\\s]", " ").split("\\s+")) { // TODO: split in a smart way
			if (this.getExcludedWords().contains(word)) {
				continue;
			}
			context.write(new Text(word), this.getClassWithOne());
		}
	}
}

class NBPositiveMapper extends NBTrainMapper {

	@Override
	public Text getReviewType() {
		return NBTrainJob.POS;
	}
	
}

class NBNegativeMapper extends NBTrainMapper {

	@Override
	public Text getReviewType() {
		return NBTrainJob.NEG;
	}
	
}


