package com.ben.dl.listd;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.FileSplit;
import org.archive.io.ArchiveReader;
import org.archive.io.warc.WARCReaderFactory;

/**
 * The WARC File Record Reader processes a single compressed input.
 * The Record Reader returns a single WARC ArchiveReader that can contain
 * numerous individual documents, each document handled in a single mapper.
 *
 * @author Stephen Merity (Smerity)
 */
public class WARCFileRecordReader implements RecordReader<Text, ArchiveReader> {
	private String arPath;
	private ArchiveReader ar;
	private FSDataInputStream fsin;
	private boolean hasBeenRead = false;

	public WARCFileRecordReader(InputSplit inputSplit, JobConf conf)
			throws IOException, InterruptedException {
		FileSplit split = (FileSplit) inputSplit;
		Path path = split.getPath();
		FileSystem fs = path.getFileSystem(conf);
		fsin = fs.open(path);
		arPath = path.getName();
		ar = WARCReaderFactory.get(path.getName(), fsin, true);
	}
/*
	@Override
	public void close() throws IOException {
		fsin.close();
		ar.close();
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		// Provide the path used for the compressed file as the key
		return new Text(arPath);
	}

	@Override
	public ArchiveReader getCurrentValue() throws IOException, InterruptedException {
		// We only ever have one value to give -- the output of the compressed file
		return ar;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// Progress of reader through the data as a float
		// As each file only produces one ArchiveReader, this will be one immediately
		return hasBeenRead ? 1 : 0;
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// As each file only produces one ArchiveReader, if it has been read, there are no more
		if (hasBeenRead) {
			return false;
		}
		hasBeenRead = true;
		return true;
	}*/

	public Text createKey() {
		// TODO Auto-generated method stub
		return new Text(arPath);
	}

	public ArchiveReader createValue() {
		// TODO Auto-generated method stub
		return ar;
	}


	public boolean next(Text arg0, ArchiveReader arg1) throws IOException {
		// TODO Auto-generated method stub
		if (hasBeenRead) {
			return false;
		}
		hasBeenRead = true;
		return true;
	}
	public void close() throws IOException {
		// TODO Auto-generated method stub
		fsin.close();
		
	}
	public float getProgress() throws IOException {
		// TODO Auto-generated method stub
		return hasBeenRead ? 1 : 0;
	}
	public long getPos() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}
}
