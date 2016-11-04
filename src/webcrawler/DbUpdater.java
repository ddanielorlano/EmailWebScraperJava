package webcrawler;

import com.google.common.collect.Lists;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DbUpdater {

    private Connection connection;
    private PreparedStatement pstmt;

    private final Set<String> emailSet;
    private List<List<String>> partitionedSet;

    private final String insertString = "INSERT INTO EMAILS VALUES (?)";
    private final String db_connect_string = "jdbc:jtds:sqlserver://54.84.109.138:1433//LCM2";
    private final String db_userid = "LCM2";
    private final String db_password = "Touro123";

    public DbUpdater(Set<String> emailSet) {
        this.emailSet = emailSet;
    }

    public void InsertEmails() throws IOException {

        partitionEmailSet();

        try {
            Class.forName("net.sourceforge.jtds.jdbc.Driver");
            connection = DriverManager.getConnection(db_connect_string, db_userid, db_password);
            connection.setAutoCommit(false);
            pstmt = connection.prepareStatement(insertString);

            for (List<String> list : partitionedSet) {
                for (String s : list) {
                    pstmt.setString(1, s);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
                connection.commit();
            }

            pstmt.executeBatch();
            connection.commit();
            connection.close();
            System.out.println("Eails have been added succefully");

        } catch (ClassNotFoundException | SQLException ex) {
            System.out.println("Cannot add emails");
        }

    }

    private void partitionEmailSet() throws IOException {
        if (!emailSet.isEmpty()) {
            List<String> emailList = new ArrayList<>();
            emailList.addAll(emailSet);
            partitionedSet = Lists.partition(emailList, 100);
        } else {
            System.out.println("emailSet is empty, getting from file");
            getEmailsFromFile();
        }
    }

    private void getEmailsFromFile() throws FileNotFoundException, IOException {

        try (BufferedReader reader = new BufferedReader(new FileReader("emails.txt"))) {
            String line = reader.readLine();
            while (line != null) {
                System.out.println("Adding: " + line);
                emailSet.add(line);
                line = reader.readLine();
            }
            partitionEmailSet();
        }
    }

    public static void main(String[] args) throws IOException {
    }
}
