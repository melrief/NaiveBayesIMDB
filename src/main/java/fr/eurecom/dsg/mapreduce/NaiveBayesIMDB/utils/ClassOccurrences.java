package fr.eurecom.dsg.mapreduce.NaiveBayesIMDB.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Class with the number of occurrences
 */
public class ClassOccurrences implements Writable {

	private Text classID;
	private long occurrences;
	
	public ClassOccurrences() { }
	
	public ClassOccurrences(Text classID, long occurrences) {
		this.classID = classID;
		this.occurrences = occurrences;
	}
	
	public Text getClassID() {
		return this.classID;
	}
	
	public long getOccurrences() {
		return this.occurrences;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		this.classID.write(out);
		out.writeLong(this.occurrences);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.classID = new Text();
		this.classID.readFields(in);
		this.occurrences = in.readLong();
	}

}
