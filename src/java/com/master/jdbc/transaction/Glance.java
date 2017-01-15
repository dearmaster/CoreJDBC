package com.master.jdbc.transaction;

import com.master.jdbc.ConnectionManager;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Glance {

    private static final Glance instance = new Glance();
    private static final int MAX_TRY = 1;
    private boolean DEPOSIT_FINISH_FLAG = false;
    private boolean WITHDRAW_FINISH_FLAG = false;

    private Glance() {
        Connection conn = ConnectionManager.getConnection();
        try {
            Statement stmt = conn.createStatement();
            stmt.execute("drop table if exists jdbc_transaction");
            stmt.execute("create table jdbc_transaction (id int not null, name varchar(20) unique not null, balance float null)");
            stmt.execute("insert into jdbc_transaction(id, name, balance) values (1, 'lily', 200)");
            ResultSet rs = stmt.executeQuery("select * from jdbc_transaction");
            while (rs.next()) {
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
            while (rs.next()) {
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
        System.out.println("connection retrieved - withdraw");
        try {
            conn.setAutoCommit(false);
            conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        try {
            Statement stmt = conn.createStatement();
            System.out.println("withdrawing....");
            stmt.execute("update jdbc_transaction set balance = balance - 10");
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            conn.commit();
            System.out.println("withdrew 10....");
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectionManager.closeConnection(conn);
        }
    }

    public void deposit() {
        Connection conn = ConnectionManager.getConnection();
        System.out.println("connection retrieved - deposit");
        try {
            conn.setAutoCommit(false);
            conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        try {
            Statement stmt = conn.createStatement();
            System.out.println("depositing....");
            stmt.execute("update jdbc_transaction set balance = balance + 10");
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            conn.commit();
            System.out.println("deposited 10....");
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
        while (!DEPOSIT_FINISH_FLAG && !WITHDRAW_FINISH_FLAG) {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        display();
    }

    class DepositThread extends Thread {
        @Override
        public void run() {
            for(int i=1; i<=MAX_TRY; i++) {
                deposit();
            }
            DEPOSIT_FINISH_FLAG = true;
        }
    }

    class WithDrawThread extends Thread {
        @Override
        public void run() {
            for(int i=1; i<=MAX_TRY; i++) {
                withdraw();
            }
            WITHDRAW_FINISH_FLAG = true;
        }
    }

}