package com.tangxi.flink.main;

import com.tangxi.flink.source.SourceFromMySQL;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName:Main2
 * @Description: TODO
 * @Author: tangxi
 * @Date: 2021/8/13
 **/
public class Main2 {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(new SourceFromMySQL()).print();

        env.execute("Flink add data sourc");
    }
}
