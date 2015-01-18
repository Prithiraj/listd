package com.ben.dl.listd;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapred.FileInputFormat;
import org.archive.io.ArchiveReader;

/**
 * Minimal implementation of FileInputFormat for WARC files.
 * Hadoop is told that splitting these compressed files is not possible.
 *
 * @author Stephen Merity (Smerity)
 */
public class WARCFileInputFormat extends FileInputFormat<Text, ArchiveReader> {

	
	protected boolean isSplitable(JobContext context, Path filename) {
		// As these are compressed files, they cannot be (sanely) split
		return false;
	}

	@Override
	public RecordReader<Text, ArchiveReader> getRecordReader(InputSplit arg0,
			JobConf arg1, Reporter arg2) throws IOException {
		// TODO Auto-generated method stub
		try {
			return new WARCFileRecordReader(arg0, arg1);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
}
