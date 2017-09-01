package main;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.Reducer.TaskContext;
import java.io.IOException;
import java.util.Iterator;

public class FriendsCombiner extends ReducerBase {
	private Record result;

	public void setup(TaskContext context) throws IOException {
		this.result = context.createMapOutputValueRecord();
	}

	public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
		long count;
		Record val;
		for (count = 0L; values.hasNext(); count += ((Long) val.get(0)).longValue()) {
			val = (Record) values.next();
			if (0L == ((Long) val.get(0)).longValue()) {
				count = 0L;
				break;
			}
		}

		this.result.set(new Object[]{Long.valueOf(count)});
		context.write(key, this.result);
	}

	public void cleanup(TaskContext context) throws IOException {
	}
}