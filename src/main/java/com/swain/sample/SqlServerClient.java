package com.swain.sample;


import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;


public class SqlServerClient {
    private String driver = "net.sourceforge.jtds.jdbc.Driver";
    private String server = "";
    private String database = "";
    private String user = "";
    private String password = "";
    Connection connection = null;

    public SqlServerClient(String server, String database, String user, String password) {
        this.server = server;
        this.database = database;
        this.user = user;
        this.password = password;
    }

    public boolean openConnection() {
       try {
             Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            String url = "XXXX";
            connection = DriverManager.getConnection(url);
            DatabaseMetaData dbm = connection.getMetaData();
            try (ResultSet rs = dbm.getTables(null, null, "%", new String[] { "TABLE" })) {
                while (rs.next()) {
                    System.out.println(rs.getString("TABLE_NAME"));
                }
            }
            return true;
       } catch (ClassNotFoundException  | SQLException e) {
       //} catch ( SQLException e) {
        e.printStackTrace();
      }
        return false;
    }

    public void closeConnection() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private List<String> getAllTableName() {
        try (Statement stmt = connection.createStatement()) {
            String sql = "select * from sysobjects where xtype='U' order by name";
            ResultSet rs = stmt.executeQuery(sql);
            ArrayList<String> ret = new ArrayList<>();
            while (rs.next()) {
                ret.add(rs.getString(1));
            }
            return ret;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void printAllTableName() {
        List<String> allTables = getAllTableName();
        for (String table : allTables) {
            System.out.println(table);
        }
    }

    public void printAllTableStructure() {
        List<String> allTables = getAllTableName();
        try (Statement stmt = connection.createStatement()) {
            for (String table : allTables) {
                System.out.println(table);
                String sql = "select top 1 * from " + table;
                ResultSet res = stmt.executeQuery(sql);
                ResultSetMetaData rsmd = res.getMetaData();
                int columnCount = rsmd.getColumnCount();
                for(int i=1; i<=columnCount; ++i){
                    System.out.print(rsmd.getColumnName(i) + "\t");
                }
                System.out.println();
                while (res.next()) {
                    for (int i = 1; i <= columnCount; i++) {
                        System.out.print(res.getString(i) + "\t");
                    }
                    System.out.println();
                }
                System.out.println();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void query(String sql) {
        try (Statement stmt = connection.createStatement()) {
            ResultSet res = stmt.executeQuery(sql);
            ResultSetMetaData rsmd = res.getMetaData();
            int columnCount = rsmd.getColumnCount();
            for(int i=1; i<=columnCount; ++i){
                System.out.print(rsmd.getColumnName(i) + "\t");
            }
            System.out.println();
            while (res.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    System.out.print(res.getString(i) + "\t");
                }
                System.out.println();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        final int LENGTH = 4;
        String[] input = new String[LENGTH];
        String[] inputTips = {"Please input server name:", "Please input database name:",
                "Please input user name:", "Please input password:"};
        int i = 0;
        while (i < args.length && i < LENGTH) {
            input[i] = args[i];
            ++i;
        }

        try (Scanner sc = new Scanner(System.in)) {
            while (i < LENGTH) {
                System.out.println(inputTips[i]);
                input[i] = sc.nextLine();
                ++i;
            }

            SqlServerClient ssc = null;
            try {
                ssc = new SqlServerClient(input[0], input[1], input[2], input[3]);
                if (!ssc.openConnection())
                    System.exit(1);
                while (true) {
                    System.out.println("SQL|list|Quit:");
                    String sql = sc.nextLine();
                    if (sql.equalsIgnoreCase("quit"))
                        break;
                    else if (sql.equalsIgnoreCase("list"))
                        ssc.printAllTableStructure();
                    else if (!sql.trim().equals("")){
                        ssc.query(sql);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                ssc.closeConnection();
            }

        }
    }
}