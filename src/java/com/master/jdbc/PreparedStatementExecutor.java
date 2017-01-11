package com.master.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/***执行动态SQL，通常通过PreparedStatement实例实现**/
public class PreparedStatementExecutor {

    private static PreparedStatementExecutor instance;

    private PreparedStatementExecutor() {
        /**Init through statement DDL**/
        StatementExecutor se = StatementExecutor.getInstance();
        String DDL_DROP_TABLE = "drop table if exists jdbc_employee";
        String DDL_CREATE_TABLE = "create table jdbc_employee(id int not null, name varchar(20) null, birthday date null, primary key(id), unique (name))";
        se.execute(DDL_DROP_TABLE);
        se.execute(DDL_CREATE_TABLE);
    }

    public static PreparedStatementExecutor getInstance() {
        if(null == instance) {
            synchronized(PreparedStatementExecutor.class) {
                if(null == instance) {
                    instance = new PreparedStatementExecutor();
                }
            }
        }
        return instance;
    }

    private static final String DML_INSERT = "insert into jdbc_employee(id, name, birthday) values (?, ?, ?)";

    public void insert(Employee employee) {
        Connection conn = ConnectionManager.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = conn.prepareStatement(DML_INSERT);
            pstmt.setInt(1, employee.getId());
            pstmt.setString(2, employee.getName());
            pstmt.setDate(3, new java.sql.Date(employee.getBirthday().getTime()));
            pstmt.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectionManager.closeConnection(conn);
        }
    }

    private static final String DML_SELECT_ALL = "select * from jdbc_employee";

    public void queryAll() {
        Connection conn = ConnectionManager.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = conn.prepareStatement(DML_SELECT_ALL);
            ResultSet rs = pstmt.executeQuery();
            while(rs.next()) {
                Integer id = rs.getInt("id");
                String name = rs.getString("name");
                Date birthday = rs.getDate("birthday");
                Employee employee = new Employee(id, name, birthday);
                System.out.println(employee);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectionManager.closeConnection(conn);
        }
    }

    private static final String DML_SELECT_BY_NAME = "select * from jdbc_employee where name = ?";

    public void getByName(String name) {
        Connection conn = ConnectionManager.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = conn.prepareStatement(DML_SELECT_BY_NAME);
            pstmt.setString(1, name);
            ResultSet rs = pstmt.executeQuery();
            while(rs.next()) {
                Integer id = rs.getInt("id");
                Date birthday = rs.getDate("birthday");
                Employee employee = new Employee(id, name, birthday);
                System.out.println(employee);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectionManager.closeConnection(conn);
        }
    }

    private static final String DML_UPDATE = "update jdbc_employee set name = ? where name = ?";

    public void update() {
        Connection conn = ConnectionManager.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = conn.prepareStatement(DML_UPDATE);
            pstmt.setString(1, "updated name");
            pstmt.setString(2, "Jim");
            int affectedRows = pstmt.executeUpdate();
            System.out.println("update affectedRows: " + affectedRows);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectionManager.closeConnection(conn);
        }
    }


    private class Employee {
        private Integer id;
        private String name;
        private Date birthday;

        public Employee(Integer id, String name, Date birthday) {
            this.id = id;
            this.name = name;
            this.birthday = birthday;
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
        public Date getBirthday() {
            return birthday;
        }
        public void setBirthday(Date birthday) {
            this.birthday = birthday;
        }

        @Override
        public String toString() {
            return "Employee{" +
                    "id=" + id +
                    ", name='" + name + '\'' +
                    ", birthday=" + birthday +
                    '}';
        }
    }

    public Employee[] mockEmployees() throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Employee[] employees = {
                new Employee(1, "lily", sdf.parse("1988-09-15")),
                new Employee(2, "Lucy", sdf.parse("1987-07-11")),
                new Employee(3, "Kevin", sdf.parse("1998-05-12")),
                new Employee(4, "Jack", sdf.parse("1990-04-22")),
                new Employee(5, "Jim", sdf.parse("1994-03-29"))
        };
        return employees;
    }

    public static void main(String[] args) {
        PreparedStatementExecutor executor = getInstance();
        executor.process();
    }

    public void process() {
        try {
            Employee[] employees = mockEmployees();
            for(Employee employee : employees) {
                insert(employee);
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
        queryAll();

        getByName("Jim");

        update();

        getByName("Jim");

    }

}