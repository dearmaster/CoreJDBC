package com.master.jdbc.transaction;

import com.master.jdbc.ConnectionManager;

import java.sql.*;

/**
 * Seems can't mock up the situation of transaction issues in java code,
 * may need to demo in procedure
 */
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
            while (rs.next()) {
                System.out.println(rs.getInt(1) + ", " + rs.getString(2) + ", " + rs.getFloat(3));
            }
            System.out.println("table jdbc_transaction created");
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectionManager.closeConnection(conn);
        }
    }

    public Float getBalanceByName(Connection conn, String name) {
        PreparedStatement pstmt;
        try {
            pstmt = conn.prepareStatement("select balance from jdbc_transaction where name = ?");
            pstmt.setString(1, name);
            ResultSet rs = pstmt.executeQuery();
            rs.next();
            return rs.getFloat(1);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static Glance getInstance() {
        return instance;
    }

    public static void main(String[] args) {
        Glance glance = getInstance();
        glance.process();
    }

    private void process() {
        Thread depositThread = new DepositThread("lily", new Float[] {10f, 10f, 10f, 10f, 10f});
        Thread withdrawThread = new WithDrawThread("lily", new Float[] {10f, 10f, 10f, 10f, 10f});
        depositThread.start();
        withdrawThread.start();
    }

    class DepositThread extends Thread {

        private String name;
        private Float[] deals;

        public DepositThread(String name, Float[] deals) {
            this.name = name;
            this.deals = deals;
        }

        @Override
        public void run() {
            Connection conn = ConnectionManager.getConnection();
            PreparedStatement pstmt;
            try {
                Thread.sleep(200);
                conn.setAutoCommit(false);
                conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
                for(int i=0; i<deals.length; i++) {
                    if(deals[i] == null)
                        continue;
                    //System.out.println("deposit operation " + (i + 1) + ": Before depositing " + deals[i] + ", the balance is " + getBalanceByName(conn, name));
                    pstmt = conn.prepareStatement("update jdbc_transaction set balance = balance + ? where name = ?");
                    pstmt.setFloat(1, deals[i]);
                    pstmt.setString(2, name);
                    pstmt.executeUpdate();
                    System.out.println("deposit operation " + (i + 1) + ": After depositing " + deals[i] + ", the balance is " + getBalanceByName(conn, name));
                    try {
                        Thread.sleep(8);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    if(i == 2) {
                        try {
                            Thread.sleep(2000);
                            System.out.println("--------------------");
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
                conn.rollback();
            } catch (SQLException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                ConnectionManager.closeConnection(conn);
            }
        }
    }

    class WithDrawThread extends Thread {

        private String name;
        private Float[] deals;

        public WithDrawThread(String name, Float[] deals) {
            this.name = name;
            this.deals = deals;
        }

        @Override
        public void run() {
            Connection conn = ConnectionManager.getConnection();
            PreparedStatement pstmt;
            try {
                Thread.sleep(200);
                conn.setAutoCommit(false);
                conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
                for(int i=0; i<deals.length; i++) {
                    if(deals[i] == null)
                        continue;
                    //System.out.println("withdraw operation " + (i + 1) + ": Before withdrawing " + deals[i] + ", the balance is " + getBalanceByName(conn, name));
                    pstmt = conn.prepareStatement("update jdbc_transaction set balance = balance - ? where name = ?");
                    pstmt.setFloat(1, deals[i]);
                    pstmt.setString(2, name);
                    pstmt.executeUpdate();
                    System.out.println("withdraw operation " + (i + 1) + ": After withdrawing " + deals[i] + ", the balance is " + getBalanceByName(conn, name));
                    try {
                        Thread.sleep(2);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                conn.commit();
            } catch (SQLException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                ConnectionManager.closeConnection(conn);
            }
        }
    }

}