package fr.eurecom.dsg.mapreduce.NaiveBayesIMDB;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public abstract class NBTestMapper extends Mapper<LongWritable, Text
																				        , Text, BooleanWritable>	{
	
	private double positiveScore = 0f;
	private double negativeScore = 0f;
	private Map<String,Double> posWordsScores = new HashMap<String,Double>();
	private Map<String,Double> negWordsScores = new HashMap<String,Double>();

	public abstract Text getReviewType();

	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		
		Path[] localCacheFiles = DistributedCache.getLocalCacheFiles(conf);
		
		String line;
		Set<String> vocabulary = new HashSet<String>();
		double positiveCounter = 0;
		double negativeCounter = 0;
		double totClassInstances = 0;
		Map<String,Long> wordsTotCounter = new HashMap<String,Long>();
		Map<String,Long> negWordsCounter = new HashMap<String,Long>();
		Map<String,Long> posWordsCounter = new HashMap<String,Long>();
		
		for (Path path : localCacheFiles) {
		  System.out.println("Reading cache file " + path);
			BufferedReader reader = new BufferedReader(new FileReader(path.toString()));
			while ((line = reader.readLine()) != null) {
				if (line.startsWith(NBTrainJob.CLASS_KEY.toString())) {
					String[] words = line.substring(NBTrainJob.CLASS_KEY.getLength())
																													 .split("\t");
					long classCounter = Long.valueOf(words[1]);
					if (words[0].equals(NBTrainJob.NEG.toString())) {
						negativeCounter = classCounter;
					}
					else if (words[0].equals(NBTrainJob.POS.toString())) {
						positiveCounter = classCounter;
					}
					totClassInstances += classCounter;
			  } else {
					String[] words = line.split("\t");
					if (words.length != 2) {
					  System.out.println("Ignoring cache file " + path + ": bad format");
					  break;
					}
					String[] wordClass = words[0].split(NBTrainJob.WORD_CLASS_SEP);
	         if (wordClass.length != 2) {
	            System.out.println("Ignoring cache file " + path + ": bad format");
	            break;
	          }
					String word = wordClass[0];
					String classStr = wordClass[1];
					Long counter = Long.valueOf(words[1]) + 1;
					
 					if (classStr.equals(NBTrainJob.NEG.toString())) {
 						negWordsCounter.put(word,counter);
 						if (!posWordsCounter.containsKey(word)) {
 						  posWordsCounter.put(word,0l);
 						}
 					}
 					else if (classStr.equals(NBTrainJob.POS.toString())) {
 						posWordsCounter.put(word,counter);
 						if (!negWordsCounter.containsKey(word)) {
              negWordsCounter.put(word,0l);
            }
 					}
 					
 					if (wordsTotCounter.containsKey(word)) {
 						wordsTotCounter.put(word,wordsTotCounter.get(word) + counter);
 					}
 					else {
 						wordsTotCounter.put(word,counter);
 					}
 					
					vocabulary.add(word);
				}
			}
			reader.close();
		}
		
		this.negativeScore = Math.log(negativeCounter / totClassInstances);
		this.positiveScore = Math.log(positiveCounter / totClassInstances);
		
		int vocabularySize = vocabulary.size();
		
		for (Entry<String,Long> wordCounter : posWordsCounter.entrySet()) {
			String word = wordCounter.getKey();
			double counter = wordCounter.getValue().doubleValue();
			double wordProb = counter/(wordsTotCounter.get(word) + vocabularySize);
			this.posWordsScores.put(word, Math.log(wordProb));
		}
		
		for (Entry<String,Long> wordCounter : negWordsCounter.entrySet()) {
			String word = wordCounter.getKey();
			double counter = wordCounter.getValue().doubleValue();
			double wordProb = counter/(wordsTotCounter.get(word) + vocabularySize);
			this.negWordsScores.put(word, Math.log(wordProb));
		}
	}
	
	@Override
	protected void map(LongWritable lineOffset, Text text, Context context)
																								 throws IOException,
																												InterruptedException {
		String sline = text.toString().trim();
		double negativeScore = this.negativeScore;
		double positiveScore = this.positiveScore;
		for (String word : sline.split("\\s+")) {
			if (this.posWordsScores.containsKey(word)) {
				positiveScore += this.posWordsScores.get(word);
				negativeScore += this.negWordsScores.get(word);
			}
		}
		
		BooleanWritable result = new BooleanWritable(false);
		if (this.getReviewType().equals(NBTrainJob.POS) && positiveScore > negativeScore) {
			result = new BooleanWritable(true);
		}
		else if (this.getReviewType().equals(NBTrainJob.NEG) && positiveScore < negativeScore) {
			result = new BooleanWritable(true);
		}
		context.write(this.getReviewType(), result);
	}
}

class NBTestPositiveMapper extends NBTestMapper {

	@Override
	public Text getReviewType() {
		return NBTrainJob.POS;
	}

}

class NBTestNegativeMapper extends NBTestMapper {

	@Override
	public Text getReviewType() {
		return NBTrainJob.NEG;
	}
	
}