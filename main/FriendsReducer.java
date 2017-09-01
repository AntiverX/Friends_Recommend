package main;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.io.LongWritable;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.Reducer.TaskContext;
import java.io.IOException;
import java.util.Iterator;

public class FriendsReducer extends ReducerBase {
	private LongWritable sum = new LongWritable();
	private Text user1 = new Text();
	private Text user2 = new Text();
	private Record result = null;

	public void setup(TaskContext context) throws IOException {
		this.result = context.createOutputRecord();
	}

	public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
		System.out.println(key.get(0));

		int count;
		Record user;
		for (count = 0; values.hasNext(); count = (int) ((long) count + ((Long) user.get(0)).longValue())) {
			user = (Record) values.next();
			if (0L == ((Long) user.get(0)).longValue()) {
				count = 0;
				break;
			}
		}

		if (count > 0) {
			this.sum.set((long) count);
			String user1 = key.get(0).toString();
			String[] users = user1.split(" ");
			this.user1.set(users[0]);
			this.user2.set(users[1]);
			this.result.set(0, this.user1);
			this.result.set(1, this.user2);
			this.result.set(2, this.sum);
			context.write(this.result);
		}

	}

	public void cleanup(TaskContext context) throws IOException {
	}
}