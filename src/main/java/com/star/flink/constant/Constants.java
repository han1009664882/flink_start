package com.star.flink.constant;

/**
 *
 * @author hanxingxing-001
 */
public class Constants {
    public static final String SMALLEST="smallest";
    public static final String LARGEST="largest";

    public static final String SYSCODE = "sysCode";
    public static final String OGN_TOPIC = "ognTopic";
    public static final String OGN_PARTITION = "ognPartition";
    public static final String OGN_OFFSET = "ognOffset";
    public final static String DMGR_PROCESS_START_TIME = "dmgrProcessStartTime";
    public final static String DMGR_PROCESS_END_TIME = "dmgrProcessEndTime";

    public static final String TEST_BASIC2_SERVERS = "10.182.83.222:21005,10.182.83.223:21005,10.182.83.224:21005,10.182.83.225:21005,10.182.83.226:21005";
    public static final String PRODUCT_SERVERS = "10.180.180.132:21005,10.180.180.133:21005,10.180.180.134:21005,10.180.180.135:21005,10.180.180.136:21005,10.180.180.137:21005,10.180.180.138:21005,10.180.180.139:21005,10.180.180.150:21005,10.180.180.151:21005,10.180.180.152:21005,10.180.180.153:21005,10.180.180.154:21005,10.180.180.155:21005,10.180.180.156:21005";

    public static final String TEST_KAFKA_SERVERS = "10.182.143.147:21005,10.182.143.148:21005,10.182.143.146:21005";

    public static final String RGEX_TABLENAME = "\"tableName\":\"(.*?)\"";
    public static final String RGEX_LOADERTIME = "\"loaderTime\":\"(.*?)\"";
    public static final String RGEX_ROWID = "\"rowid\":\"(.*?)\"";
    public static final String RGEX_SCN = "\"scn\":\"(.*?)\"";
    public static final String RGEX_SEQID = "\"seqid\":\"(.*?)\"";
    public static final String RGEX_OPTS = "\"opTs\":\"(.*?)\"";
}
