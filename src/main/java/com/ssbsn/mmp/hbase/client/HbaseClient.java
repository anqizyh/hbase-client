package com.ssbsn.mmp.hbase.client;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.filter.Filter;


/**
 * 〈一句话功能简述〉<br>
 * 〈功能详细描述〉
 *
 * @author 
 * @see [相关类/方法]（可选）
 * @since [产品/模块版本] （可选）
 */
public interface HbaseClient {
    
    /**
     * 
     * 功能描述: <br>
     * 〈功能详细描述〉
     *
     * @param tableName  表名
     * @param rowKey   键
     * @param requiredType 类类型
     * @return
     * @throws IOException 
     * @see [相关类/方法](可选)
     * @since [产品/模块版本](可选)
     */
    <T> T queryForObject(String tableName, String rowKey, Class<T> requiredType);
    
    /**
     * 
     * @param tableName
     * @param startRow
     * @param endRow
     * @param requiredType
     * @return
     */
    <T> List<T> queryForList(String tableName, String startRow,String endRow,Filter filter ,Class<T> requiredType);
}
