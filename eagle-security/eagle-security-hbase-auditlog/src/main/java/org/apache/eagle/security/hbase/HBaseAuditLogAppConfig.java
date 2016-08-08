package org.apache.eagle.security.hbase;

import com.typesafe.config.Config;
import org.apache.eagle.app.Configuration;
import org.apache.eagle.app.annotation.Property;

public class HBaseAuditLogAppConfig extends Configuration {
    public final static String SPOUT_TASK_NUM = "topology.numOfSpoutTasks";
    public final static String PARSER_TASK_NUM = "topology.numOfParserTasks";
    public final static String JOIN_TASK_NUM = "topology.numOfJoinTasks";
    public final static String SINK_TASK_NUM = "topology.numOfSinkTasks";

    @Property(SPOUT_TASK_NUM)
    private int spoutTaskNum;

    @Property(PARSER_TASK_NUM)
    private int parserTaskNum;

    @Property(JOIN_TASK_NUM)
    private int joinTaskNum;

    @Property(SINK_TASK_NUM)
    private int sinkTaskNum;

    public HBaseAuditLogAppConfig(Config config) {
        super(config);
    }

    public int getSpoutTaskNum() {
        return spoutTaskNum;
    }

    public void setSpoutTaskNum(int spoutTaskNum) {
        this.spoutTaskNum = spoutTaskNum;
    }

    public int getParserTaskNum() {
        return parserTaskNum;
    }

    public void setParserTaskNum(int parserTaskNum) {
        this.parserTaskNum = parserTaskNum;
    }

    public int getJoinTaskNum() {
        return joinTaskNum;
    }

    public void setJoinTaskNum(int joinTaskNum) {
        this.joinTaskNum = joinTaskNum;
    }

    public int getSinkTaskNum() {
        return sinkTaskNum;
    }

    public void setSinkTaskNum(int sinkTaskNum) {
        this.sinkTaskNum = sinkTaskNum;
    }
}