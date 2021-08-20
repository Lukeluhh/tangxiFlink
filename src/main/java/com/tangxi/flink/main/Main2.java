package com.tangxi.flink.main;

import com.tangxi.flink.model.Student;
import com.tangxi.flink.source.SourceFromMySQL;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static sun.misc.Version.print;

/**
 * @ClassName:Main2
 * @Description: TODO
 * @Author: tangxi
 * @Date: 2021/8/13
 **/
public class Main2 {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Student> studentDataStreamSource = env.addSource(new SourceFromMySQL());
        studentDataStreamSource.print();

        env.execute("Flink add data sourc");
    }
}
