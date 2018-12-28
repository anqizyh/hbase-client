package com.sbsn.mmp.hbase.base;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 〈一句话功能简述〉<br>
 * 〈功能详细描述〉
 *
 * @author 
 * @see [相关类/方法]（可选）
 * @since [产品/模块版本] （可选）
 */
public class HBaseConfFactory {

    /**
     * 配置文件地址
     */
    private static final String CONFIG_FILE = "/hbase.properties";

    /**
     * 配置文件
     */
    private static Configuration configuration = null;

    /**
     * 连接
     */
    private static HConnection conn = null;
    
    /**
     * zkserver
     */
    private static String quorum = null;
   
    /**
     * 端口号 
     */
    private static String clientPort = null;
    
    /**
     * 用户
     */
    private static String hadoopUser=null;
    
    /**
     * 用户组
     */
    private static String hadoopGroupUser=null;

    /**
     * 日志
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseConfFactory.class);

    static {
        try {
            configuration = HBaseConfiguration.create();
            initConfig(configuration);
            conn = HConnectionManager.createConnection(configuration);
           
       
        } catch (IOException e) {
            LOGGER.error("hbase connection error:" + e);
        }
    }

    public  static void initConfig(Configuration configuration) {
        InputStream inputStream = HBaseConfFactory.class.getResourceAsStream(CONFIG_FILE);
        if (inputStream != null) {
            try {
                Properties properties = new Properties();
                properties.load(inputStream);
                quorum = (String) properties.get("quorum");
                clientPort = (String) properties.get("clientPort");
                hadoopUser = (String) properties.get("hadoopGroupUser");
                hadoopGroupUser = (String) properties.get("hadoopGroupUser");
                //设置配置参数
                configuration.set("hbase.zookeeper.quorum", quorum);   
                configuration.set("hbase.zookeeper.property.clientPort", clientPort);  
                
                //设置用户权限
                System.setProperty("HADOOP_USER_NAME", hadoopUser);
                System.setProperty("HADOOP_GROUP_NAME", hadoopGroupUser);
            } catch (Exception e) {
                LOGGER.error("init file error" + e);
            } finally {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    LOGGER.error("close stream error", e);
                }
            }
        }

    };

    public static HConnection getInstance() {
        if (conn == null) {
            try {
                configuration = HBaseConfiguration.create();
                initConfig(configuration);
                conn = HConnectionManager.createConnection(configuration);
              
            } catch (IOException e) {
                LOGGER.error("hbase connection error:" + e);
            }
        }

        return conn;
    }

}
