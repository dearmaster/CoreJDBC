package com.master.jdbc.transaction;

import com.master.jdbc.ConnectionManager;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Glance {

    private static final Glance instance = new Glance();

    private Glance() {
        Connection conn = ConnectionManager.getConnection();
        try {
            Statement stmt = conn.createStatement();
            stmt.execute("drop table if exists jdbc_transaction");
            stmt.execute("create table jdbc_transaction (id int not null, name varchar(20) unique not null, balance float null)");
            stmt.execute("insert into jdbc_transaction(id, name, balance) values (1, 'lily', 200)");
            ResultSet rs = stmt.executeQuery("select * from jdbc_transaction");
            while(rs.next()) {
                System.out.println(rs.getInt(1) + ", " + rs.getString(2) + ", " + rs.getFloat(3));
            }
            System.out.println("table created");
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectionManager.closeConnection(conn);
        }
    }

    public void display() {
        Connection conn = ConnectionManager.getConnection();
        try {
            System.out.println(conn.getTransactionIsolation());
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select * from jdbc_transaction");
            while(rs.next()) {
                System.out.println(rs.getInt(1) + ", " + rs.getString(2) + ", " + rs.getFloat(3));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectionManager.closeConnection(conn);
        }
    }

    public void withdraw() {
        Connection conn = ConnectionManager.getConnection();
        try {
            conn.setAutoCommit(false);
            conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            Statement stmt = conn.createStatement();
            stmt.execute("update jdbc_transaction set balance = balance - 10");
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectionManager.closeConnection(conn);
        }
    }

    public void deposit() {
        Connection conn = ConnectionManager.getConnection();
        try {
            conn.setAutoCommit(false);
            conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            Statement stmt = conn.createStatement();
            stmt.execute("update jdbc_transaction set balance = balance + 10");
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectionManager.closeConnection(conn);
        }
    }

    public static Glance getInstance() {
        return instance;
    }

    public static void main(String[] args) {
        Glance glance = getInstance();
        glance.process();
    }

    private void process() {
        Thread depositThread = new DepositThread();
        Thread withdrawThread = new WithDrawThread();
        depositThread.start();
        withdrawThread.start();
        try {
            depositThread.join();
            withdrawThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        display();
    }

    class DepositThread extends Thread {
        @Override
        public void run() {
            deposit();
        }
    }

    class WithDrawThread extends Thread {
        @Override
        public void run() {
            withdraw();
        }
    }

}