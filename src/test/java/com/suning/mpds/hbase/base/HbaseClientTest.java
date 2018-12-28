/*
 * Copyright (C), 2002-2016, 
 * FileName: HbaseClientTest.java
 * Author:   
 * Date:     2016年1月11日 下午2:08:12
 * Description: //模块目的、功能描述      
 * History: //修改记录
 * <author>      <time>      <version>    <desc>
 * 修改人姓名             修改时间            版本号                  描述
 */
package com.suning.mpds.hbase.base;

import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.sbsn.mmp.hbase.demo.MemberInfoDTO;
import com.ssbsn.mmp.hbase.client.HbaseClient;
import com.ssbsn.mmp.hbase.client.HbaseClientImpl;

/**
 * 〈一句话功能简述〉<br>
 * 〈功能详细描述〉
 *
 * @author 
 * @see [相关类/方法]（可选）
 * @since [产品/模块版本] （可选）
 */
public class HbaseClientTest {

    public static void main(String[] args) {
//        long time=System.currentTimeMillis();
//        testQuery("ns_sospdm:mpds_member_info","");
//        long time2=System.currentTimeMillis();
//        System.out.println(time2-time);
 //       testQuery("ns_sospdm:mpds_member_info",StringUtils.reverse("123456789"));
//        long time3=System.currentTimeMillis();
//        System.out.println(time3-time2);
    	
    	
    	testQueryList("ns_sospdm:mpds_member_info",StringUtils.reverse("123456789"),StringUtils.reverse("123456789")+"-");

    }

    public static void testQuery(String tableName, String rowKey) {

        HbaseClient client = new HbaseClientImpl();
        MemberInfoDTO dto = client.queryForObject(tableName, rowKey, MemberInfoDTO.class);
        
        System.out.println("age="+dto.getAge());

    }
    
    public static void testQueryList(String tableName, String startRow,String endRow) {

        HbaseClient client = new HbaseClientImpl();
        List<MemberInfoDTO> list= client.queryForList(tableName, startRow, endRow, null, MemberInfoDTO.class);
        
        System.out.println("age="+list.get(0).getAge());
    }

}
