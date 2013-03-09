package fr.eurecom.dsg.mapreduce.NaiveBayesIMDB;
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class NBTestReducer extends Reducer<Text,BooleanWritable
																				  ,Text,DoubleWritable> {

	@Override
	protected void reduce(Text classID
										  , Iterable<BooleanWritable> values
										  , Context context) throws IOException
										  												, InterruptedException {
		double tot = 0, num_correct = 0;
		for (BooleanWritable isCorrect : values) {
			tot += 1;
			if (isCorrect.get()) {
				num_correct += 1;
			}
		}
		
		if (tot == 0) {
			context.write(classID, new DoubleWritable(1));
		} else {
			context.write(classID, new DoubleWritable(num_correct/tot));
		}
	}

}
