package com.master.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class ConnectionManager {

    public static Properties getProps() {
        //InputStream is = ConnectionManager.class.getResourceAsStream("dataSource.properties"); //the properties files should be placed under the same package
        InputStream is = ConnectionManager.class.getClassLoader().getResourceAsStream("dataSource.properties");
        Properties props = new Properties();
        try {
            props.load(is);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return props;
    }

    public static Connection getConnection() {
        Properties props = getProps();
        String driver = props.getProperty("MYSQL_DRIVER");
        String url = props.getProperty("MYSQL_HONEY_DB_URL");
        String username = props.getProperty("MYSQL_HONEY_DB_USERNAME");
        String password = props.getProperty("MYSQL_HONEY_DB_PASSWORD");
        Connection conn = null;
        try {
            Class.forName(driver);
            conn = DriverManager.getConnection(url, username, password);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    public static void main(String[] args) {
        Connection conn = getConnection();
        System.out.println(conn);
        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}