package com.founder.utils;

import com.founder.dsj.bean.entity.DsjDataCompareEntity;
import com.founder.object.TableStructure;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataBillUtil {
    public static List<Map<String, Object>> fetchTableData(JdbcTemplate jdbcTemplate, String tableName, List<String> columns) {
        String columnList = String.join(", ", columns);
        String sql = "SELECT " + columnList + " FROM " + tableName;
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            Map<String, Object> row = new HashMap<>();
            for (String column : columns) {
                row.put(column, rs.getObject(column));
            }
            return row;
        });

    }
    public static <T> List<T> getIntersection(List<T> list1, List<T> list2) {
        List<T> result = new ArrayList<>();
        Map<T, Integer> countMap = new HashMap<>();

        // 统计 list2 中每个元素的出现次数
        for (T element : list2) {
            countMap.put(element, countMap.getOrDefault(element, 0) + 1);
        }

        // 遍历 list1，找出交集元素
        for (T element : list1) {
            if (countMap.containsKey(element) && countMap.get(element) > 0) {
                result.add(element);
                countMap.put(element, countMap.get(element) - 1);
            }
        }

        return result;
    }

    public static Map<String, Map<String, Object>> generateFingerprints(List<Map<String, Object>> data) {
        Map<String, Map<String, Object>> fingerprints = new HashMap<>();
        for (Map<String, Object> row : data) {
            String fingerprint = generateRowFingerprint(row);
            fingerprints.put(fingerprint, row);
        }
        return fingerprints;
    }

    public static String generateRowFingerprint(Map<String, Object> row) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Object> entry : row.entrySet()) {
            sb.append(entry.getKey()).append("=").append(entry.getValue()).append("|");
        }
        return sb.toString();
    }

    public static JdbcTemplate createJdbcTemplate(DsjDataCompareEntity table){
        return new JdbcTemplate(createDataSource(table));
    }


    public static TableStructure getTableStructure(JdbcTemplate jdbcTemplate, String tableName){

        TableStructure tableStructure=new TableStructure();

        String sql="select * from " + tableName + " limit 1";

        jdbcTemplate.query(sql,rs->{
            ResultSetMetaData metaData=rs.getMetaData();
            int columnCount=metaData.getColumnCount();
            for (int i = 1; i <= columnCount; i++) {
                String columnName=metaData.getColumnName(i);
                int columnType=metaData.getColumnType(i);
                String typeName=metaData.getColumnTypeName(i);
                tableStructure.addColumn(columnName,columnType,typeName);
            }
        });
        return tableStructure;
    }

    //创建数据源
    public static DataSource createDataSource(DsjDataCompareEntity datasource){

        String host= datasource.getHost();
        String port= datasource.getPort();
        String userName= datasource.getUserName();
        String passWord= datasource.getPassword();
        String database=datasource.getDatabase();

        StringBuilder sb=new StringBuilder();

        sb.append("jdbc:mysql://")
                .append(host)
                .append(":")
                .append(port)
                .append("/")
                .append(database);

        String jdbcUrl=new String(sb);

        HikariDataSource dataSource=new HikariDataSource();

        dataSource.setJdbcUrl(jdbcUrl);
        dataSource.setUsername(userName);
        dataSource.setPassword(passWord);
        dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");

        return dataSource;
    }
}
