package com.master.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**执行静态SQL，通常通过Statement实例实现**/
public class StatementExecutor {

    private static final StatementExecutor instance = new StatementExecutor();

    private StatementExecutor() {}

    public static StatementExecutor getInstance() {
        return instance;
    }

    private static final String DDL_CREATE_TABLE = "create table jdbc_demo(id integer not null, name varchar(30) null, gender varchar(2))";
    private static final String DDL_DROP_TABLE = "drop table jdbc_demo";
    private static final String[] DML_INSERT_MOCK_UP_DATA = {
            "insert into jdbc_demo(id, name, gender) values (1, 'Lily', '女')",
            "insert into jdbc_demo(id, name, gender) values (2, 'Lucy', '女')",
            "insert into jdbc_demo(id, name, gender) values (3, 'Kevin', '男')"
    };
    private static final String DML_UPDATE_DATA = "update jdbc_demo set name = 'Poly', gender = '男' where id = 1";
    private static final String DML_DELETE_DATA = "delete from jdbc_demo where name = 'Lucy'";

    public void execute(String sql) {
        Connection conn = ConnectionManager.getConnection();
        try {
            Statement stmt = conn.createStatement();
            stmt.execute(sql);
            System.out.println("Query SUCCESS: " + sql);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectionManager.closeConnection(conn);
        }
    }

    private static final String DML_SELECT_DATA = "select * from jdbc_demo";

    public void executeQuery(String sql) {
        Connection conn = ConnectionManager.getConnection();
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            System.out.println(rs.getMetaData());
            while(rs.next()) {
                int id = rs.getInt("id");
                String name = rs.getString("name");
                String gender = rs.getString("gender");
                User user = new User(id, name, gender);
                System.out.println(user);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectionManager.closeConnection(conn);
        }
    }


    public void createTable() {
        this.execute(DDL_CREATE_TABLE);
    }

    public void dropTable() {
        this.execute(DDL_DROP_TABLE);
    }

    public void insertData() {
        for(String sql : DML_INSERT_MOCK_UP_DATA) {
            this.execute(sql);
        }
    }

    public void updateDate() {
        this.execute(DML_UPDATE_DATA);
    }

    public void selectData() {
        this.executeQuery(DML_SELECT_DATA);
    }

    public void deleteData() {
        this.execute(DML_DELETE_DATA);
    }

    private class User {

        private Integer id;
        private String name;
        private String gender;

        public User(Integer id, String name, String gender) {
            this.id = id;
            this.name = name;
            this.gender = gender;
        }

        public Integer getId() {
            return id;
        }
        public void setId(Integer id) {
            this.id = id;
        }
        public String getName() {
            return name;
        }
        public void setName(String name) {
            this.name = name;
        }
        public String getGender() {
            return gender;
        }
        public void setGender(String gender) {
            this.gender = gender;
        }

        @Override
        public String toString() {
            return "User{" +
                    "id=" + id +
                    ", name='" + name + '\'' +
                    ", gender='" + gender + '\'' +
                    '}';
        }
    }

    public static void main(String[] args) {
        StatementExecutor executor = getInstance();
        executor.dropTable();
        executor.createTable();
        executor.insertData();
        executor.updateDate();
        executor.deleteData();
        executor.selectData();
    }

}