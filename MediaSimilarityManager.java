import java.sql.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

class Assignment {
    public String user_name;
    public String password;
    public String server_name;


    public Assignment(String userName, String password) {
        this.user_name = userName;
        this.password = password;
        this.server_name = "132.72.64.124";
       }

    public Connection getConnection()  {
        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        String connectionUrl = "jdbc:sqlserver://" + server_name + ":1433;databaseName=" + user_name + ";user=" + user_name + ";" +
                "password=" + password + ";encrypt=false;";

        try {
            return DriverManager.getConnection(connectionUrl);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void fileToDataBase(String filePath) {
        // Define connection to SQL SERVER:
        Connection con = null;
        PreparedStatement ps = null;
        try {
            con = getConnection();
            BufferedReader br = new BufferedReader(new FileReader(filePath));
            String insert_MI = "INSERT INTO MediaItems (MID, PROD_YEAR, TITLE) VALUES (?, ?, ?)";
            ps = con.prepareStatement(insert_MI);

            String line;
            int MID = 0;

            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                String title = values[0];

                MID++;

                int prodYear = Integer.parseInt(values[1].trim());


                ps.setInt(1, MID);
                ps.setInt(2, prodYear);
                ps.setString(3, title);

                ps.executeUpdate();

            }


            ps.close();

            con.close();
            br.close();


        } catch (SQLException | IOException e) {
            e.printStackTrace();}

        finally {
            try {
                if (ps != null) {
                    ps.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                if (con != null) {
                    con.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }


    public void calculateSimilarity() {
        Connection con = null;
        PreparedStatement simStmt = null;
        try {
            // Connect to the database
            con = getConnection();


            // Calculate the maximal distance
            CallableStatement maxDistStmt = con.prepareCall("{? = call dbo.MaximalDistance()}");
            maxDistStmt.registerOutParameter(1, Types.SMALLINT);
            maxDistStmt.execute();
            int maxDist = maxDistStmt.getInt(1);

            // Calculate and insert the similarity values
            simStmt = con.prepareStatement("INSERT INTO Similarity (MID1, MID2, SIMILARITY)" +
                    " SELECT m1.MID, m2.MID, dbo.SimCalculation(m1.MID, m2.MID, ?)" +
                    " FROM MediaItems m1, MediaItems m2" +
                    " WHERE m1.MID < m2.MID");

            simStmt.setInt(1, maxDist);
            simStmt.executeUpdate();

            con.close();
            simStmt.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            try {
                if (simStmt != null) {
                    simStmt.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                if (con != null) {
                    con.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public void printSimilarities(long mid) {
        Connection con = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            // Connect to the database
            con = getConnection();

            // Retrieve similar items
            stmt = con.prepareStatement("SELECT m.Title, s.Similarity " +
                    "FROM MediaItems m " +
                    "JOIN Similarity s ON m.MID = s.MID2 OR m.MID = s.MID1 " +
                    "WHERE (s.MID1 = ? OR s.MID2 = ?) AND s.Similarity >= 0.3 AND m.title != (SELECT TOP 1 m.title FROM MediaItems m WHERE m.MID = ?) " +
                    "ORDER BY s.Similarity ASC");
            stmt.setLong(1, mid);
            stmt.setLong(2, mid);
            stmt.setLong(3, mid);
            rs = stmt.executeQuery();

            // Print results
            while (rs.next()) {
                String title = rs.getString("Title");
                float similarity = rs.getFloat("Similarity");
                System.out.println(title + " - Similarity: " + similarity);
            }

            // Close resources
            rs.close();
            stmt.close();
            con.close();

        } catch (SQLException | NumberFormatException e) {
            e.printStackTrace();}


        finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                if (con != null) {
                    con.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                if (rs != null) {
                    rs.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }


}

