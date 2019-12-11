package com.example.cloudcompute;
import org.apache.commons.collections.map.HashedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 使用 JdbcTemplate 操作 Hive
 */
@RestController
@RequestMapping("/hive2")
public class HiveJdbcTemplateController {

    private static final Logger logger = LoggerFactory.getLogger(HiveJdbcTemplateController.class);

//    @Autowired
//    @Qualifier("hiveDruidTemplate")
//    private JdbcTemplate hiveDruidTemplate;

    @Autowired
    @Qualifier("hiveJdbcTemplate")
    private JdbcTemplate hiveJdbcTemplate;

    /**
     * 示例：创建新表
     */
    @RequestMapping("/goods/create")
    public String createGoodsTable() {
        StringBuffer sql = new StringBuffer("CREATE TABLE IF NOT EXISTS ");
        sql.append("goods");
        sql.append("(goods_id INT, goods_name STRING, goods_info STRING, goods_pic STRING, goods_price INT, goods_number INT)");
//        sql.append("ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' "); // 定义分隔符
//        sql.append("STORED AS TEXTFILE"); // 作为文本存储
        logger.info("Running: " + sql);
        String result = "Create table successfully...";
        try {
            hiveJdbcTemplate.execute(sql.toString());
        } catch (DataAccessException dae) {
            result = "Create table encounter an error: " + dae.getMessage();
            logger.error(result);
        }
        return result;
    }

    @RequestMapping("/user/create")
    public String createUserTable() {
        StringBuffer sql = new StringBuffer("CREATE TABLE IF NOT EXISTS ");
        sql.append("users");
        sql.append("(user_id INT, user_name STRING, user_password STRING, user_phone STRING, user_address STRING)");
//        sql.append("ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' "); // 定义分隔符
//        sql.append("STORED AS TEXTFILE "); // 作为文本存储
        logger.info("Running: " + sql);
        String result = "Create table successfully...";
        try {
            hiveJdbcTemplate.execute(sql.toString());
        } catch (DataAccessException dae) {
            result = "Create table encounter an error: " + dae.getMessage();
            logger.error(result);
        }
        return result;
    }

    @RequestMapping("/order/create")
    public String createOrderTable() {
        StringBuffer sql = new StringBuffer("CREATE TABLE IF NOT EXISTS ");
        sql.append("orders");
        sql.append("(user_name STRING, goods_name STRING, goods_number INT)");
//        sql.append("ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' "); // 定义分隔符
//        sql.append("STORED AS TEXTFILE"); // 作为文本存储
        logger.info("Running: " + sql);
        String result = "Create table successfully...";
        try {
            hiveJdbcTemplate.execute(sql.toString());
        } catch (DataAccessException dae) {
            result = "Create table encounter an error: " + dae.getMessage();
            logger.error(result);
        }
        return result;
    }

    /**
     * 示例：将Hive服务器本地文档中的数据加载到Hive表中
     */
    @RequestMapping("/table/load")
    public String loadIntoTable() {
        String filepath = "/home/hadoop/user_sample.txt";
        String sql = "load data local inpath '" + filepath + "' into table user_sample";
        String result = "Load data into table successfully...";
        try {
            // hiveJdbcTemplate.execute(sql);
            hiveJdbcTemplate.execute(sql);
        } catch (DataAccessException dae) {
            result = "Load data into table encounter an error: " + dae.getMessage();
            logger.error(result);
        }
        return result;
    }

    /**
     * 示例：向Hive表中添加数据
     */
    @RequestMapping("/table/insert")
    public String insertIntoTable() {
        String sql = "INSERT INTO TABLE  goods(goods_id, goods_name , goods_info , goods_pic, goods_price , goods_number) VALUES(1,'苹果','红的','dfada',15,32)";
//        String sql2= "INSERT INTO TABLE  goods(goods_id, goods_name , goods_info , goods_pic, goods_price , goods_number) VALUES(2,'梨子','鸭的','dfada',15,32)";
//        String sql3= "INSERT INTO TABLE  goods(goods_id, goods_name , goods_info , goods_pic, goods_price , goods_number) VALUES(3,'桃子','长毛的','dfada',15,32)";
//        String sql4= "INSERT INTO TABLE  goods(goods_id, goods_name , goods_info , goods_pic, goods_price , goods_number) VALUES(4,'葡萄','绿的','dfada',15,32)";
//        String sql5= "INSERT INTO TABLE  goods(goods_id, goods_name , goods_info , goods_pic, goods_price , goods_number) VALUES(5,'榴莲','香的','dfada',15,32)";
//        String sql6="INSERT INTO TABLE users(user_name,user_password,user_phone,user_address) VALUES ('ghy','123456','18721923502','20号楼533')";
//        String sql7="INSERT INTO TABLE orders(user_name,goods_name,goods_number) VALUES ('ghy','榴莲',3)";
//        String a="香蕉";
//        int b=15;
//        String sql8="INSERT INTO TABLE  goods(goods_id, goods_name , goods_info , goods_pic, goods_price , goods_number) VALUES(6,'"+a+"','香的','dfada',"+b+",32)";
        String result = "Insert into table successfully...";

        try {
             hiveJdbcTemplate.execute(sql);
//            hiveJdbcTemplate.execute(sql2);
//            hiveJdbcTemplate.execute(sql3);
//            hiveJdbcTemplate.execute(sql4);
//            hiveJdbcTemplate.execute(sql5);
//            hiveJdbcTemplate.execute(sql6);
//            hiveJdbcTemplate.execute(sql7);
//            hiveJdbcTemplate.execute(sql8);
        } catch (DataAccessException dae) {
            result = "Insert into table encounter an error: " + dae.getMessage();
            logger.error(result);
        }
        return result;
    }

    @RequestMapping("/table/test")
    public String testTable() {
        String sql ="insert overwrite table goods select * from goods where goods_id<>1";
        String result = "Insert into table successfully...";
        try {
            hiveJdbcTemplate.execute(sql);
            sql="SELECT * FROM goods";
            hiveJdbcTemplate.execute(sql);
        } catch (DataAccessException dae) {
            result = "Insert into table encounter an error: " + dae.getMessage();
            logger.error(result);
        }
        return result;
    }
    /**
     * 示例：删除表
     */
    @RequestMapping("/table/delete")
    public String delete(String tableName) {
        String sql = "DROP TABLE IF EXISTS "+tableName;
        String result = "Drop table successfully...";
        logger.info("Running: " + sql);
        try {
            // hiveJdbcTemplate.execute(sql);
            hiveJdbcTemplate.execute(sql);
        } catch (DataAccessException dae) {
            result = "Drop table encounter an error: " + dae.getMessage();
            logger.error(result);
        }
        return result;
    }

    @PostMapping("/goods/select")
    public Map<String,Object> goodsSelect(){
        Map<String,Object> map=new HashMap<>();
        String sql ="SELECT * FROM goods";
        String result;
        List<Map<String, Object>> rows;
        try {
            rows= hiveJdbcTemplate.queryForList(sql);
            map.put("goods_list", rows);
        } catch (DataAccessException dae) {
            result = "Select table encounter an error: " + dae.getMessage();
            map.put("goods_list", result);
        }
        return map;
    }

    @PostMapping("/order/select")
    public Map<String,Object> selectOrder(@RequestBody Map<String,String> iMap){
        Map<String,Object> map=new HashMap<>();
        String result ="前端返回参数无效";
        if(iMap.get("user_name")!=null){
            String userName=iMap.get("user_name");
            String sql="SELECT * FROM orders o WHERE o.user_name=?";
            List<Map<String, Object>> rows;
            try {
                rows= hiveJdbcTemplate.queryForList(sql,userName);
                map.put("order_list", rows);
            } catch (DataAccessException dae) {
                result = "Select table encounter an error: " + dae.getMessage();
                map.put("order_list", result);
            }
            return map;
        }
        else{
            map.put("order_list", result);
            return map;
        }
    }

    @PostMapping("/user/update")
    public Map<String,Object> userUpdate(@RequestBody Map<String,String> iMap) {
        Map<String,Object> map=new HashMap<>();
        String result ="前端返回参数无效";
        if(iMap.get("user_name")!=null){
            String userName=iMap.get("user_name");
            String userPhone=iMap.get("user_phone");
            String useradd=iMap.get("user_address");
            try {
                    String sql="select * from users where user_name='"+userName+"'";
                    Map<String,Object> user=hiveJdbcTemplate.queryForMap(sql);
                    sql="insert overwrite table users select * from users where user_name<>'"+userName+"'";
                    hiveJdbcTemplate.execute(sql);
                    sql = "INSERT INTO TABLE users(user_name,user_password,user_phone,user_address)" +
                            " VALUES ('"+user.get("user_name")+"','"+user.get("user_password")+
                            "','"+userPhone+"','"+useradd+"')";
                    hiveJdbcTemplate.execute(sql);
            } catch (DataAccessException dae) {
                result = "Update table encounter an error: " + dae.getMessage();
                map.put("result", result);
            }
        }
        else{
            map.put("result", result);
        }
        return map;
    }

    @PostMapping("/user/register")
    public Map<String,Object> userRegister(@RequestBody Map<String,String> iMap) {
        Map<String, Object> map = new HashMap<>();
        String result = "前端返回参数无效";
        if(iMap.get("user_name")!=null){
            String userName=iMap.get("user_name");
            String userPhone=iMap.get("user_phone");
            String useradd=iMap.get("user_address");
            String userPass=iMap.get("user_password");
            String sql="INSERT INTO TABLE users(user_name,user_password,user_phone,user_address) VALUES ('"+userName+"','"+userPass+"','"+userPhone+"','"+useradd+"')";
            try {
                hiveJdbcTemplate.execute(sql);
            } catch (DataAccessException dae) {
                result = "Insert table encounter an error: " + dae.getMessage();
                map.put("result", result);
            }
        }
        else{
            map.put("result", result);
        }
        return map;
    }

    @PostMapping("/user/login")
    public Map<String,Object> userLogin(@RequestBody Map<String,String> iMap) {
        Map<String, Object> map = new HashMap<>();
        String result = "前端返回参数无效";
        if(iMap.get("user_name")!=null){
            String userName=iMap.get("user_name");
            String userPass=iMap.get("user_password");
            Map<String,Object> row=null;
            String sql="SELECT * FROM users WHERE user_name='"+userName+"'";
            try {
                row=hiveJdbcTemplate.queryForMap(sql);
                if(row==null){
                    result="用户名错误";
                    map.put("result", result);
                }
                else{
                    if(row.get("user_password")!=userPass){
                        result="密码错误";
                        map.put("result", result);
                    }
                    else{
                        result="登陆成功";
                        map.put("result", result);
                    }
                }
            } catch (DataAccessException dae) {
                result = "Insert table encounter an error: " + dae.getMessage();
                map.put("result", result);
            }
        }
        else{
            map.put("result", result);
        }
        return map;
    }

    @PostMapping("/goods/buy")
    public Map<String,Object> goodsBuy(@RequestBody Map<String,String> iMap) {
        Map<String,Object> map=new HashMap<>();
        String result ="前端返回参数无效";
        if(iMap.get("user_name")!=null){
            String userName=iMap.get("user_name");
            String goodsName=iMap.get("goods_name");
            int goodsNum= Integer.parseInt(iMap.get("goods_number"));
            String sql0="SELECT * FROM goods WHERE goods_name='"+goodsName+"'";
            Map<String,Object> row=hiveJdbcTemplate.queryForMap(sql0);
            goodsNum=Integer.parseInt((String) row.get("goods_number"))-goodsNum;
            String sql2="INSERT INTO TABLE orders(user_name,goods_name,goods_number) VALUES ('"+userName+"','"+goodsName+"',"+goodsNum+")";
            try {
                hiveJdbcTemplate.execute(sql2);
                String sql="select * from goods where goods_name='"+goodsName+"'";
                Map<String,Object> goods=hiveJdbcTemplate.queryForMap(sql);
                sql="insert overwrite table goods select * from goods where goods_name='"+goodsName+"'";
                hiveJdbcTemplate.execute(sql);
                sql = "INSERT INTO TABLE  goods(goods_id,goods_name , goods_info , goods_pic, goods_price , goods_number) " +
                        "VALUES("+goods.get("goods_id")+",'"+goods.get("goods_name")+ "','"+goods.get("goods_info")+
                        "','"+goods.get("goods_pic")+"',"+goods.get("goods_price")+","+goodsNum+")";
                hiveJdbcTemplate.execute(sql);
            } catch (DataAccessException dae) {
                result = "Update table encounter an error: " + dae.getMessage();
                map.put("result", result);
            }
        }
        else{
            map.put("result", result);
        }
        return map;
    }
}
