package main;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.Mapper.TaskContext;
import java.io.IOException;
import java.util.Arrays;

public class FriendsMapper extends MapperBase {
	private Record key;
	private Record value;

	public void setup(TaskContext context) throws IOException {
		this.key = context.createMapOutputKeyRecord();
		this.value = context.createMapOutputValueRecord();
		System.out.println("TaskID:" + context.getTaskID().toString());
	}

	public void map(long recordNum, Record record, TaskContext context) throws IOException {
		String user = record.get(0).toString();
		String all = user + " " + record.get(1).toString();
		String[] arr = all.split(" ");
		Arrays.sort(arr);
		int len = arr.length;

		for (int i = 0; i < len - 1; ++i) {
			for (int j = i + 1; j < len; ++j) {
				this.key.set(new Object[]{arr[i] + " " + arr[j]});
				if (!arr[i].equals(user) && !arr[j].equals(user)) {
					this.value.set(new Object[]{Long.valueOf(1L)});
					context.write(this.key, this.value);
				} else {
					this.value.set(new Object[]{Long.valueOf(0L)});
					context.write(this.key, this.value);
				}
			}
		}

	}

	public void cleanup(TaskContext context) throws IOException {
	}
}