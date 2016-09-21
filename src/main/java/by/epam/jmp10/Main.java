package by.epam.jmp10;


import java.sql.*;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Vitali on 20.09.2016.
 */
public class Main {
    static final String DB_URL = "jdbc:mysql://localhost:3306/jmp?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";

    static final String USER = "root";
    static final String PASS = "";
    static final int MAX_TABLES = 25;
    static final int MAX_THREADS = 25;
    static final int MAX_COLUMNS = 10;
    static final int MAX_ROWS = 100;

    public static void main(String[] args) {
        AtomicInteger tablesNumber = new AtomicInteger(new Random().nextInt(MAX_TABLES) + 1);
        int threadsNumber = new Random().nextInt(MAX_THREADS) + 1;

        for (int i = 0; i < threadsNumber; i++) {
            new Thread(() -> {
                Connection conn = null;
                Statement stmt = null;
                long startTime = System.currentTimeMillis();
                try{
                    conn = DriverManager.getConnection(DB_URL, USER, PASS);
                    conn.setAutoCommit(false);
                    stmt = conn.createStatement();
                    int tableNumber = tablesNumber.getAndDecrement();
                    while(tableNumber > 0){
                        String tableName = "Table_" + tableNumber;
                        int numberColumns = new Random().nextInt(MAX_COLUMNS) + 1;
                        StringBuilder sql = new StringBuilder("CREATE TABLE " + tableName + "(");
                        for (int j = 0; j < numberColumns; j++) {
                            sql.append("Column_" + j + " varchar(255)");
                            if(j != numberColumns-1){
                                sql.append(",");
                            }
                        }
                        sql.append(");");
                        stmt.execute(sql.toString());

                        int rowsNumber = new Random().nextInt(MAX_ROWS) + 1;
                        for (int j = 0; j < rowsNumber; j++) {
                            sql = new StringBuilder("INSERT INTO " + tableName + " VALUES (");
                            for (int k = 0; k < numberColumns; k++) {
                                sql.append("'VALUE_" + k + "'");
                                if(k != numberColumns-1){
                                    sql.append(",");
                                }
                            }
                            sql.append(");");
                            stmt.execute(sql.toString());
                        }
                        tableNumber = tablesNumber.getAndDecrement();
                    }
                    conn.commit();
                }catch(SQLException se){
                    se.printStackTrace();
                }catch(Exception e){
                    e.printStackTrace();
                }finally{
                    try{
                        if(stmt!=null)
                            stmt.close();
                    }catch(SQLException se2){
                    }
                    try{
                        if(conn!=null)
                            conn.close();
                    }catch(SQLException se){
                        se.printStackTrace();
                    }
                }
                long endTime = System.currentTimeMillis();
                System.out.println("Execution time of " + Thread.currentThread().getName() + " : " + (endTime - startTime));
            }).start();
        }
    }
}
