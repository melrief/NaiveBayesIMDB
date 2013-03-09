package fr.eurecom.dsg.mapreduce.NaiveBayesIMDB;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import fr.eurecom.dsg.mapreduce.NaiveBayesIMDB.utils.ClassOccurrences;



public class NBTrainCombiner extends
						 Reducer<Text, ClassOccurrences, Text, ClassOccurrences> {

	@Override
	protected void reduce(Text text
											, Iterable<ClassOccurrences> classesInstances
											, Context context) throws IOException,
																								InterruptedException {
		long posInstances = 0;
		long negInstances = 0;
		for (ClassOccurrences classInstances : classesInstances) {
			if (classInstances.getClassID().equals(NBTrainJob.POS)) {
				posInstances += classInstances.getOccurrences();
			}
			else {
				negInstances += classInstances.getOccurrences();
			}
		}
		
		context.write(text,new ClassOccurrences(NBTrainJob.POS, posInstances));
		context.write(text,new ClassOccurrences(NBTrainJob.NEG, negInstances));

	}

}
