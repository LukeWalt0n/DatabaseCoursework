import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.Scanner;
import java.util.ArrayList;
import java.io.BufferedReader;
import java.util.List;
import java.io.*;
import java.util.*;

public class Database{

    private List<String> customerHeaders;
    private List<String> customerData;
    
    /**
     * Return a connection to the desired database.
     */
    private Connection connect(){

        Connection conn = null;
        Scanner s = new Scanner(System.in);

        try{
            //We will have the DB stored in the same directory.
            String url = "jdbc:sqlite:TRY.db";

            //Get the connection to the DB.
            conn = DriverManager.getConnection(url);
            //System.out.println("Connection to SQLite has been established");
        }
        catch(SQLException e){
            System.out.println(e.getMessage());
        }
        
        return conn;
    }

    private String[] getHeaders(String csv){
        String[] tempArr = new String[100];
        try{
            File file = new File(csv);
            FileReader fr = new FileReader(file);
            BufferedReader br = new BufferedReader(fr);
            String line = " ";
            
            
            //Read the first line for the headers.
            line = br.readLine();
            tempArr = line.split(",");
            System.out.println(Arrays.toString(tempArr) + "\n");
        }
        catch(Exception e){
            System.out.println("Problem reading file");
        }
        return tempArr;
    }

    

    private void insertPlayersData(String csv){

        String[] data = new String[1000];
        Connection conn = null;
        try{
            File file = new File(csv);
            FileReader fr = new FileReader(file);
            BufferedReader br = new BufferedReader(fr);
            String line = " ";
            boolean firstLine = true;

            while((line = br.readLine()) != null){
                
                if(firstLine){
                    firstLine = false;
                            continue;
                }
                else{
                    //System.out.println(line + "\n");
                    data = line.split(",");
                    try{
                        conn = this.connect();
                        PreparedStatement p = conn.prepareStatement("INSERT INTO Player (AccountNumber, Forename, Surname, EmailAddress, CName) VALUES (?,?,?,?,?)");
                        p.setInt(1, Integer.parseInt(data[0]));
                        p.setString(2, data[1]);
                        p.setString(3, data[2]);
                        p.setString(4, data[3]);
                        p.setString(5, data[6]);
                        p.executeUpdate();

                    }
                    catch(SQLException ex){
                        System.out.println(ex.getMessage());
                    }


                }
            }
        }
        catch(Exception e){
            System.out.println("Error reading file");
        }
        finally{
            try{
                if(conn != null){
                    conn.close();
                    
                }
            }
            catch(SQLException ex){
                System.out.println(ex.getMessage());
            }
        }
    }

    private void insertCharacterData(String csv){

        String[] data = new String[1000];
        Connection conn = null;
        try{
            File file = new File(csv);
            FileReader fr = new FileReader(file);
            BufferedReader br = new BufferedReader(fr);
            String line = " ";
            boolean firstLine = true;

            while((line = br.readLine()) != null){
                
                if(firstLine){
                    firstLine = false;
                            continue;
                }
                else{
                    //System.out.println(line + "\n");
                    data = line.split(",");
                    
                    try{
                        conn = this.connect();
                        PreparedStatement p = conn.prepareStatement("INSERT INTO Character (CharacterName, "
                        + "CreationDate, ExpiryDate, PlayerAccountNumber, AttackScore, MaxHealth, Level, ExperiencePoints, "
                        + "Health, DefenceScore) VALUES (?,?,?,?,?,?,?,?,?,?)");

                        p.setString(1, data[6]);
                        p.setString(2, data[4]);
                        p.setString(3, data[5]);
                        p.setInt(4, Integer.parseInt(data[0]));
                        p.setInt(5, Integer.parseInt(data[12]));
                        p.setInt(6, Integer.parseInt(data[10]));
                        p.setInt(7, Integer.parseInt(data[8]));
                        p.setInt(8, Integer.parseInt(data[9]));
                        p.setInt(9, Integer.parseInt(data[11]));
                        p.setInt(10, Integer.parseInt(data[13]));
                        p.executeUpdate();

                    }
                    catch(SQLException ex){
                        System.out.println(ex.getMessage());
                    }


                }
            }
        }
        catch(Exception e){
            System.out.println("Error reading file");
        }
        finally{
            try{
                if(conn != null){
                    conn.close();
                    
                }
            }
            catch(SQLException ex){
                System.out.println(ex.getMessage());
            }
        }
    }

    private void insertCombatData(String csv){

        String[] data = new String[1000];
        Connection conn = null;
        
        try{
            File file = new File(csv);
            FileReader fr = new FileReader(file);
            BufferedReader br = new BufferedReader(fr);
            String line = " ";
            boolean firstLine = true;

            while((line = br.readLine()) != null){
                
                if(firstLine){
                    firstLine = false;
                            continue;
                }
                else{
                    //System.out.println(line + "\n");
                    data = line.split(",");
                    
                    try{
                        conn = this.connect();
                        PreparedStatement p = conn.prepareStatement("INSERT INTO HasCombatActivities (BattleNum, "
                        + "BattleDate, Attacker, Defender, Weapon, Result, CharacterName) "
                        + "VALUES (?,?,?,?,?,?,?)");

                        p.setString(1, data[1]);
                        p.setString(2, data[0]);
                        p.setString(3, data[2]);
                        p.setString(4, data[3]);
                        p.setString(5, data[4]);
                        p.setString(6, data[5]);
                        p.setString(7, data[2]);
                        p.executeUpdate();

                    }
                    catch(SQLException ex){
                        System.out.println(ex.getMessage());
                        
                    }


                }
            }
        }
        catch(Exception e){
        }
        finally{
            try{
                if(conn != null){
                    conn.close();
                    
                    
                    
                }
            }
            catch(SQLException ex){
                System.out.println(ex.getMessage());
            }
        }
    }

    private void insertItemData(String csv){

        String[] data = new String[1000];
        Connection conn = null;
        try{
            File file = new File("Items.csv");
            FileReader fr = new FileReader(file);
            BufferedReader br = new BufferedReader(fr);
            String line = " ";
            boolean firstLine = true;

            while((line = br.readLine()) != null){
                
                if(firstLine){
                    firstLine = false;
                            continue;
                }
                else{
                    //System.out.println(line + "\n");
                    data = line.split(",");
                    
                    try{
                        conn = this.connect();
                        PreparedStatement p = conn.prepareStatement("INSERT INTO CarriesItem (ItemName, "
                        + "Amount, CharacterName, AttackScore, Range, ItemType, BodyPart, HealthScore, ManaScore, DefenceScore) "
                        + "VALUES (?,?,?,?,?,?,?,?,?,?)");

                        p.setString(1, data[1]);
                        try{
                            p.setInt(2, Integer.parseInt(data[6]));
                        }
                        catch(Exception e){
                            p.setInt(2, 0);
                        }
                        p.setString(3, data[0]);
                        try{
                            p.setInt(4, Integer.parseInt(data[8]));
                        }
                        catch(Exception e){
                            p.setInt(4, 0);
                        }
                        
                        try{
                            p.setInt(5, Integer.parseInt(data[4]));
                        }
                        catch(Exception e){
                            p.setInt(5, 0);
                        }

                        p.setString(6, data[2]);
                        p.setString(7, data[14]);

                        
                        try{
                            p.setInt(8, Integer.parseInt(data[9]));
                        }
                        catch(Exception e){
                            p.setInt(8, 0);
                        }
                        try{
                            p.setInt(9, Integer.parseInt(data[10]));
                        }
                        catch(Exception e){
                            p.setInt(9, 0);
                        }
                        try{
                            p.setInt(10, Integer.parseInt(data[7]));
                        }
                        catch(Exception e){
                            p.setInt(10, 0);
                        }
                        p.executeUpdate();

                    }
                    catch(SQLException ex){
                        System.out.println(ex.getMessage());
                    }


                }
            }
        }
        catch(Exception e){
            
            System.out.println("Error reading file ITEMS");
        }
        finally{
            try{
                if(conn != null){
                    conn.close();
                    
                }
            }
            catch(SQLException ex){
                System.out.println(ex.getMessage());
            }
        }
    }

    public void CreateTables(){

        Connection conn = null;
        try{
            
            
            conn = this.connect();
            Statement st = conn.createStatement();
            int playersQuery = st.executeUpdate(
                ("CREATE TABLE Player "+
                "(AccountNumber INTEGER NOT NULL, " 
                + "Forename VARCHAR(32), " 
                + "Surname VARCHAR(32), "
                + "EmailAddress VARCHAR(32), "
                + "CName VARCHAR(32), " 
                + "PRIMARY KEY(AccountNumber, CName), " 
                + "FOREIGN KEY (CName) REFERENCES Character(CharacterName));"
            ));


            int characterQuery = st.executeUpdate(
                (
                    "CREATE TABLE Character "
                    + "(CharacterName VARCHAR(32) NOT NULL, "
                    + "CreationDate VARCHAR(32) NOT NULL, "
                    + "ExpiryDate VARCHAR(32), "
                    + "PlayerAccountNumber INTEGER NOT NULL, "
                    + "AttackScore INTEGER NOT NULL, "
                    + "MaxHealth INTEGER NOT NULL, "
                    + "Level INTEGER NOT NULL, "
                    + "ExperiencePoints INTEGER NOT NULL, "
                    + "Health INTEGER NOT NULL, "
                    + "DefenceScore INTEGER NOT NULL, "
                    + "PRIMARY KEY (CharacterName), "
                    + "FOREIGN KEY (PlayerAccountNumber) REFERENCES PLAYER(AccountNumber) ON DELETE CASCADE)"
                )
            );

            int hasCombatTable = st.executeUpdate(
                (
                    "CREATE TABLE HasCombatActivities "
                    + "(BattleNum INTEGER NOT NULL, "
                    + "BattleDate INTEGER, "
                    + "Attacker VARCHAR(32) NOT NULL, "
                    + "Defender VARCHAR(32) NOT NULL, "
                    + "Weapon VARCHAR(32) NOT NULL, "
                    + "Result VARCHAR(32) NOT NULL, "
                    + "CharacterName VARCHAR(32) NOT NULL, "
                    + "PRIMARY KEY(BattleNum), "
                    + "FOREIGN KEY (CharacterName) REFERENCES Character(CharacterName) "
                    + "ON DELETE CASCADE)"
                )
            );

            int carriesItemTable = st.executeUpdate(
                (
                    "CREATE TABLE CarriesItem "
                    + "(ItemName VARCHAR(32) NOT NULL, "
                    + "Amount INTEGER, "
                    + "CharacterName VARCHAR(32) NOT NULL, "
                    + "AttackScore INTEGER, "
                    + "Range INTEGER, "
                    + "ItemType VARCHAR(32) NOT NULL, "
                    + "BodyPart VARCHAR(32), "
                    + "HealthScore INTEGER, "
                    + "ManaScore INTEGER, "
                    + "DefenceScore INTEGER, "
                    + "PRIMARY KEY (ItemName, CharacterName), "
                    + "FOREIGN KEY (CharacterName) REFERENCES Character(CharacterName))"
                )
            );

            int paysInvoiceTable = st.executeUpdate(
                (
                    "CREATE TABLE PaysInvoice "
                    + "(CreationDate VARCHAR(32) NOT NULL, "
                    + "AccountNumber NOT NULL, "
                    + "PRIMARY KEY (CreationDate)"
                    + "FOREIGN KEY (AccountNumber) REFERENCES Player(AccountNumber))"
                )
            );

            System.out.println("Tables Created");
        }
        catch(SQLException e){
            
            System.out.println(e.getMessage());
        }
        finally{
            try{
                if(conn != null){
                    conn.close();
                }
            }
            catch (SQLException ex){
                System.out.println(ex.getMessage());
            }
        }
        
    }

    public void query1(){
        Connection conn = null;
        try{
            conn = this.connect();
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(
                (
                    "SELECT CharacterName, count(*) AS Victories "
                    + "FROM HasCombatActivities "
                    + "WHERE Result = 'Victory' "
                    + "GROUP BY CharacterName "
                    + "ORDER BY count(*) DESC "
                    + "LIMIT 5"
                )
            );
            while(rs.next()){
                System.out.println("Executing Query 1 --- CharacterName, Victories");
                System.out.println(rs.getString(1) + ", " + rs.getInt(2));
            }
        }
        catch(SQLException e){
            System.out.println(e.getMessage());
        }
    }

    public void query2(){
        Connection conn = null;
        try{
            conn = this.connect();
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(
                (
                    "SELECT CharacterName, count(*) AS Attacks "
                    + "FROM HasCombatActivities "
                    + "WHERE Result = 'Hit' "
                    + "GROUP BY CharacterName "
                    + "HAVING count(*) > 5 "
                    + "ORDER BY count(*) DESC "
                    + "LIMIT 5"
                )
            );
            System.out.println("Executing Query 2 --- CharacterName, Attacks");
            while(rs.next()){
                
                System.out.println(rs.getString(1) + ", " + rs.getInt(2));
            }
        }
        catch(SQLException e){
            System.out.println(e.getMessage());
        }
    }

    public void query3(){
        Connection conn = null;
        try{
            conn = this.connect();
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(
                (
                    "SELECT CharacterName, count(*) AS Attacks "
                    + "FROM HasCombatActivities "
                    + "WHERE Result in ('Hit', 'Miss')"
                    + "GROUP BY CharacterName "
                    + "ORDER BY count(*) DESC "
                )
            );
            System.out.println("Executing Query 3 --- CharacterName, Attacks");
            while(rs.next()){
                
                System.out.println(rs.getString(1) + ", " + rs.getInt(2));
            }
        }
        catch(SQLException e){
            System.out.println(e.getMessage());
        }
    }

    public static void main(String[] args){
        Database db = new Database();
        db.CreateTables();
        db.insertPlayersData("Customers.csv");
        db.insertCharacterData("Customers.csv");
        db.insertCombatData("Combat.csv");
        db.insertItemData("Items.csv");

        db.query1();
        System.out.println("\n\n\n");
        db.query2();
        System.out.println("\n\n\n");
        db.query3();
        
}       

}


