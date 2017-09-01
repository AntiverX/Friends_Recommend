package main;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.RunningJob;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;
import main.FriendsCombiner;
import main.FriendsMapper;
import main.FriendsReducer;

public class FriendsDriver {
	public static void main(String[] args) throws OdpsException {
		JobConf job = new JobConf();
		job.setMapOutputKeySchema(SchemaUtils.fromString("friends:string"));
		job.setMapOutputValueSchema(SchemaUtils.fromString("cnt:bigint"));
		InputUtils.addTable(TableInfo.builder().tableName(args[0]).build(), job);
		OutputUtils.addTable(TableInfo.builder().tableName(args[1]).build(), job);
		job.setMapperClass(FriendsMapper.class);
		job.setCombinerClass(FriendsCombiner.class);
		job.setReducerClass(FriendsReducer.class);
		RunningJob rj = JobClient.runJob(job);
		rj.waitForCompletion();
	}
}