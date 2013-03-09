package fr.eurecom.dsg.mapreduce.NaiveBayesIMDB;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import fr.eurecom.dsg.mapreduce.NaiveBayesIMDB.utils.ClassOccurrences;



public class NBTrainReducer extends
		Reducer<Text, ClassOccurrences, Text, LongWritable> {

	private static final Text CLASS_POS = new Text(NBTrainJob.CLASS_KEY 
																							 + NBTrainJob.POS.toString());
	private static final Text CLASS_NEG = new Text(NBTrainJob.CLASS_KEY
																							 + NBTrainJob.NEG.toString());

	@Override
	protected void reduce(Text wordOrClass
											, Iterable<ClassOccurrences> classesInstances
											, Context context) throws IOException,
																								InterruptedException {
		long posCounter = 0;
		long negCounter = 0;
		for (ClassOccurrences classInstances : classesInstances) {
			if (classInstances.getClassID().equals(NBTrainJob.NEG)) {
				negCounter += classInstances.getOccurrences();
			}
			else if (classInstances.getClassID().equals(NBTrainJob.POS)) {
				posCounter += classInstances.getOccurrences();
			}
			else {
				System.err.println("Unknown class ID " + classInstances.getClassID());
			}
		}
		
		if (wordOrClass.equals(NBTrainJob.CLASS_KEY)) {	
			context.write(NBTrainReducer.CLASS_POS, new LongWritable(posCounter));
			context.write(NBTrainReducer.CLASS_NEG, new LongWritable(negCounter));
		} else {
			Text posKey = new Text(wordOrClass.toString() + NBTrainJob.WORD_CLASS_SEP
																										+ NBTrainJob.POS.toString());
			Text negKey = new Text(wordOrClass.toString() + NBTrainJob.WORD_CLASS_SEP
																										+ NBTrainJob.NEG.toString());
			context.write(posKey, new LongWritable(posCounter));
			context.write(negKey, new LongWritable(negCounter));
		}
	}

}
