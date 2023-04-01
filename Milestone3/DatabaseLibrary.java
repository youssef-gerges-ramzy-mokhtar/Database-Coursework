import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParsePosition;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.Scanner;
import java.io.*;
import javax.print.attribute.standard.PagesPerMinute;
import javax.swing.plaf.basic.BasicComboBoxUI.ItemHandler;
import javax.swing.text.PlainDocument;

public class DatabaseLibrary {
	private String database;
    private Connection conn;

	private String customerCSV;
	private String combatCSV;
	private String itemsCSV;

	public DatabaseLibrary(String database) {
		this.database = database;
		this.customerCSV = "Customers.csv";
		this.combatCSV = "Combat.csv";
		this.itemsCSV = "Items.csv";
	}

	private void connect() {
        conn = null;

		try {
            String url = "jdbc:sqlite:" + database + ".db";
            conn = DriverManager.getConnection(url);
            System.out.println("Connection to " + database + " has been established");
        } catch (SQLException e) {
			System.out.println("Connection Failed");
			System.out.println(e.getMessage());
		}
    }

    private void disconnect() {
        if (conn == null) return;

        try {
            conn.close();
            System.out.println("Database Closed Successfully");
        } catch (SQLException ex) {
            System.out.println("Database Didn't Close");
            System.out.println(ex.getMessage());
        }

		conn = null;
    }
	
	public void run() {
		connect();
		createTables();
		disconnect();

		connect();
		populateTables();
		disconnect();
	}

	private void createTables() {
		System.out.println("Creating Tables");
		if (conn == null) {
			System.out.println("Connection is NULL");
			return;
		}

		String[] tables = {
			// playerDDL
			"Player (Forename TEXT, Surname TEXT,email TEXT, AccountNo INTEGER NOT NULL, level INTEGER, experiencePoints INTEGER, moneyWallet INTEGER, moneyBank REAL, Name TEXT NOT NULL, PRIMARY KEY (AccountNo), FOREIGN KEY (Name) REFERENCES Owns_Character(Name) ON DELETE RESTRICT)",
			
			// ownsCharacterDDL Here I made the assumption that the character name is unique, and that to be able to read the data from the combat.csv and items.csv files and link them to the Characters
			"Owns_Character (AccountNo INTEGER NOT NULL, maxHealth INTEGER, stealthScore INTEGER, defenceScore INTEGER, type TEXT, health INTEGER, expiryDate TEXT, creationDate TEXT, attackingScore INTEGER, maneScore INTEGER, Name TEXT NOT NULL UNIQUE, PRIMARY KEY (Name, AccountNo), FOREIGN KEY (AccountNo) REFERENCES Player(AccountNo) ON DELETE CASCADE)",
			
			// combatInfoDDL
			"CombatInfo (BattleNo INTEGER NOT NULL, BattleDate TEXT, PRIMARY KEY (BattleNo))",

			// charCombatStatusDDL
			"char_combat_status (damage INTEGER, result TEXT, weapon TEXT, defender TEXT, Name TEXT NOT NULL, BattleNo INTEGER NOT NULL, PRIMARY KEY (BattleNo, Name), FOREIGN KEY (Name) REFERENCES Owns_Character(Name) ON DELETE CASCADE, FOREIGN KEY (BattleNo) REFERENCES CombatInfo(BattleNo) ON DELETE RESTRICT)",
			
			// inventoryDDL 
			"Inventory (Name TEXT NOT NULL, PRIMARY KEY (Name), FOREIGN KEY (Name) REFERENCES Owns_Character(Name) ON DELETE CASCADE)",
			
			// weaponDDL
			"Weapon (item TEXT NOT NULL,type TEXT, range INTEGER, price INTEGER, damage_points INTEGER, PRIMARY KEY (item))",
			
			// armourDDL 
			"Armour (item TEXT NOT NULL, body_part TEXT, price INTEGER, defence_score INTEGER, PRIMARY KEY (item))",
			
			// supplyDDL
			"Supply (item TEXT NOT NULL, healing_score INTEGER, price INTEGER, mana_score INTEGER, PRIMARY KEY (item))",
			
			// weapongInvDDL 
			"weapon_inv (quantity INTEGER, equipped INTEGER, Name TEXT NOT NULL, item TEXT NOT NULL, PRIMARY KEY (Name, item), FOREIGN KEY (Name) REFERENCES Inventory(Name) ON DELETE CASCADE, FOREIGN KEY (item) REFERENCES Weapon(item) ON DELETE CASCADE)",
			
			// armourInvDDL
			"armour_inv (quantity INTEGER, equipped INTEGER, Name TEXT NOT NULL, item TEXT NOT NULL, PRIMARY KEY (Name, item), FOREIGN KEY (Name) REFERENCES Inventory(Name) ON DELETE CASCADE, FOREIGN KEY (item) REFERENCES Armour(item) ON DELETE CASCADE)",
			
			// supplyInvDDL
			"supply_inv (quantity INTEGER, equipped INTEGER, Name TEXT NOT NULL, item TEXT NOT NULL, PRIMARY KEY (Name, item), FOREIGN KEY (Name) REFERENCES Inventory(Name) ON DELETE CASCADE, FOREIGN KEY (item) REFERENCES Supply(item) ON DELETE CASCADE)",
		};

		
		for (String table: tables) {
			try {
				Statement creationStatement = conn.createStatement();
				int resultOfQuery = creationStatement.executeUpdate("CREATE TABLE IF NOT EXISTS " + table);
				System.out.println(resultOfQuery + " Table created Successfully");
			} catch (SQLException e) {
				System.out.println(e.getMessage());
			}
		}

		System.out.println("Finished Creating Tables\n");
	}

	private void populateTables() {
		if (conn == null) {
			System.out.println("Connection is NULL");
			return;
		}

		System.out.println("Populting Player Table");
		populatePlayer();
		System.out.println("Finished Populating Player Table\n");

		System.out.println("Populating Owns_Character Table");
		populateOwnsCharacter();
		System.out.println("Finished Populating Owns_Character Table\n");

		System.out.println("Populating CombatInfo Table");
		populateCombatInfo();
		System.out.println("Finished Populating CombatInfo Table\n");

		System.out.println("Populating char_combat_status Table");
		populateCharCombatStatus();
		System.out.println("Finished Populating char_combat_status Table\n");
		
		System.out.println("Populating Inventory Table");
		populateInventory();
		System.out.println("Finished Populating Inventory Table\n");

		System.out.println("Populating Weapon Table");
		populateWeapon();
		System.out.println("Finished Populating Weapon Table\n");

		System.out.println("Populating Armour Table");
		populateArmour();
		System.out.println("Finished Populating Armour Table\n");

		System.out.println("Populating Supply Table");
		populateSupply();
		System.out.println("Finished Populating Supply Table\n");

		System.out.println("Populating weapon_inv Table");
		populateWeaponInv();
		System.out.println("Finished Populating weapon_inv Table\n");

		System.out.println("Populating armour_inv Table");
		populateArmourInv();
		System.out.println("Finished Populating armour_inv Table\n");
		
		System.out.println("Populating supply_inv Table");
		populateSupplyInv();
		System.out.println("Finished Populating sypply_inv Table\n");
	}

	private void populatePlayer() {
		int[] player_entries_pos = {1, 2, 3, 0, 8, 9, 17, 16, 6};
		boolean[] player_text_entries = {true, true, true, false, false, false, false, false, true};
		populateTable(customerCSV, "Player", player_entries_pos, player_text_entries, "", -1);
	}

	private void populateOwnsCharacter() {
		int[] character_entries_pos = {0, 10, 14, 13, 7, 11, 5, 4, 12, 15, 6};
		boolean[] character_text_entries = {false, false, false, false, true, false, true, true, false, false, true};
		populateTable(customerCSV, "Owns_Character", character_entries_pos, character_text_entries, "", -1);
	}

	private void populateCombatInfo() {
		int[] entries_pos = {1, 0};
		boolean[] text_entries = {false, true};
		populateTable(combatCSV, "CombatInfo", entries_pos, text_entries, "", -1);
	}

	private void populateCharCombatStatus() {
		int[] entries_pos = {6, 5, 4, 3, 2, 1};
		boolean[] text_entries = {false, true, true, true, true, false};
		populateTable(combatCSV, "char_combat_status", entries_pos, text_entries, "", -1);
	}

	private void populateInventory() {
		int[] entries_pos = {0};
		boolean[] text_entries = {true};
		populateTable(itemsCSV, "Inventory", entries_pos, text_entries, "", -1);
	}

	private void populateWeapon() {
		int[] entries_pos = {1, 3, 4, 5, 8};
		boolean[] text_entries = {true, true, false, false, false};
		populateTable(itemsCSV, "Weapon", entries_pos, text_entries, "Weapon", 2);
	}

	private void populateArmour() {
		int[] entries_pos = {1, 14, 5, 7};
		boolean[] text_entries = {true, true, false, false};
		populateTable(itemsCSV, "Armour", entries_pos, text_entries, "Armour", 2);
	}

	private void populateSupply() {
		int[] entries_pos = {1, 9, 5, 10};
		boolean[] text_entries = {true, false, false, false};
		// populateTable(itemsCSV, "Supply", entries_pos, text_entries, "Armour", 2);

		try {
			Scanner sc = new Scanner(new File(itemsCSV));
			sc.useDelimiter("\r\n");
			String row = sc.next();
			
			while (sc.hasNext()) {
				row = sc.next();
				System.out.println(row);

				String[] rowValues = row.split(",");
				if (rowValues[2].equals("Weapon") || rowValues[2].equals("Armour")) continue;
				
				String row_entry = "";
				for (int i = 0; i < entries_pos.length; i++) {
					if (entries_pos[i] >= rowValues.length) {
						row_entry += "null"; 
						if (i != entries_pos.length-1) row_entry += ", ";
						continue;
					}

					if (rowValues[entries_pos[i]].length() == 0) rowValues[entries_pos[i]] = "null";
					else if (text_entries[i]) rowValues[entries_pos[i]] = "\"" + rowValues[entries_pos[i]] + "\"";

					row_entry += rowValues[entries_pos[i]];
					if (i != entries_pos.length - 1) row_entry += ", ";
				}

				insertRow("Supply", row_entry);
				System.out.println(row_entry);
			}

			sc.close();
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}

	private void populateWeaponInv() {
		int[] entries_pos = {6, 15, 0, 1};
		boolean[] text_entries = {false, false, true, true};
		populateTable(itemsCSV, "weapon_inv", entries_pos, text_entries, "Weapon", 2);
	}

	private void populateArmourInv() {
		int[] entries_pos = {6, 15, 0, 1};
		boolean[] text_entries = {false, false, true, true};
		populateTable(itemsCSV, "armour_inv", entries_pos, text_entries, "Armour", 2);
	}

	private void populateSupplyInv() {
		int[] entries_pos = {6, 15, 0, 1};
		boolean[] text_entries = {false, false, true, true};

		try {
			Scanner sc = new Scanner(new File(itemsCSV));
			sc.useDelimiter("\r\n");
			String row = sc.next();
			
			while (sc.hasNext()) {
				row = sc.next();
				System.out.println(row);

				String[] rowValues = row.split(",");
				if (rowValues[2].equals("Weapon") || rowValues[2].equals("Armour")) continue;
				
				String row_entry = "";
				for (int i = 0; i < entries_pos.length; i++) {
					if (entries_pos[i] >= rowValues.length) {
						row_entry += "null"; 
						if (i != entries_pos.length-1) row_entry += ", ";
						continue;
					}

					else if (rowValues[entries_pos[i]].length() == 0) rowValues[entries_pos[i]] = "null";
					else if (text_entries[i]) rowValues[entries_pos[i]] = "\"" + rowValues[entries_pos[i]] + "\"";

					row_entry += rowValues[entries_pos[i]];
					if (i != entries_pos.length - 1) row_entry += ", ";
				}

				insertRow("supply_inv", row_entry);
				System.out.println(row_entry);
			}

			sc.close();
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}

	private void populateTable(String csv, String table, int[] entries_pos, boolean[] text_entries, String condition, int conditionIdx) {
		try {
			Scanner sc = new Scanner(new File(csv));
			sc.useDelimiter("\r\n");
			String row = sc.next();
			
			while (sc.hasNext()) {
				row = sc.next();
				System.out.println(row);

				String[] rowValues = row.split(",");
				if (conditionIdx != -1 && !rowValues[conditionIdx].equals(condition)) continue;
				
				String row_entry = "";
				for (int i = 0; i < entries_pos.length; i++) {
					if (entries_pos[i] >= rowValues.length) {
						row_entry += "null"; 
						if (i != entries_pos.length-1) row_entry += ", ";
						continue;
					}

					if (rowValues[entries_pos[i]].length() == 0) rowValues[entries_pos[i]] = "null";
					else if (text_entries[i]) rowValues[entries_pos[i]] = "\"" + rowValues[entries_pos[i]] + "\"";

					row_entry += rowValues[entries_pos[i]];
					if (i != entries_pos.length - 1) row_entry += ", ";
				}

				insertRow(table, row_entry);
				System.out.println(row_entry);
			}

			sc.close();
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}

	private void insertRow(String table, String row) {
		try {
			Statement creationStatement = conn.createStatement();
			int resultOfQuery = creationStatement.executeUpdate("INSERT INTO " + table + " VALUES(" + row + ")");
			System.out.println(resultOfQuery + " Record Inserted Successfully");
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
	}

	private void executeQuery(String query) {
		this.connect();
		if (conn == null) {
			System.out.println("Connection Failed");
			return;
		}

		try {
			Statement selectQuery = conn.createStatement();
			ResultSet resultSet = selectQuery.executeQuery(query);

			int columnsCnt = resultSet.getMetaData().getColumnCount();
			boolean setEmpty = true;
			while (resultSet.next()) {
				setEmpty = false;

				for (int i = 1; i <= columnsCnt; i++) {
					String colVal = resultSet.getString(i);
					System.out.print(colVal);
					
					if (i != columnsCnt) System.out.print(", ");
				}
				
				System.out.println();
			}
			
			if (setEmpty) System.out.println("Empty Set ∅");
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}

		System.out.println();
	}

	public void query1() {
		String query = 
			"SELECT ccs.Name, COUNT(*) as num_successful_attacks " +
			"FROM char_combat_status ccs " +
			"WHERE ccs.result = 'Victory' " +
			"GROUP by ccs.Name " +
			"ORDER BY num_successful_attacks DESC " + 
			"LIMIT 5";

		System.out.println("List the top 5 characters with the highest number of successful combats attacks.");
		executeQuery(query);
	}

	public void query2() {
		String query =
			"SELECT ccs.Name, COUNT(*) as num_attacks " +
			"FROM char_combat_status ccs " +
			"GROUP by ccs.Name " +
			"HAVING num_attacks > 5;";
		
		System.out.println("Print the name and total number of attacks per character having more than 5 attacks.");
		executeQuery(query);
	}

	public void query3() {
		String query = 
			"SELECT ccs.Name, COUNT(*) as num_attacks " +
			"FROM char_combat_status ccs " +
			"GROUP by ccs.Name " +
			"ORDER BY num_attacks DESC;";

		System.out.println("Order the names of characters from highest to lowest number of attacks.");
		executeQuery(query);
	}

	public void query4() {
		String query = 
			"select Player.Forename, Player.Surname, COUNT(*) " +
			"FROM Player " + 
			"INNER JOIN Owns_Character ON Player.AccountNo = Owns_Character.AccountNo " +
			"GROUP BY Player.AccountNo " +
			"HAVING COUNT(*) >= 5;";
	
		System.out.println("List the name of Players with at least 5 characters.");
		executeQuery(query);	
	}

	public void query5() {
		String query = 
			"select wi.item, COUNT(Player.AccountNo) AS num_players " +
   			"FROM weapon_inv wi " +
   			"INNER JOIN " +
   			"(Owns_Character INNER JOIN Player ON Owns_Character.ACcountNo = Player.AccountNo) " +
   			"ON Owns_Character.Name = wi.Name " +
			"GROUP BY wi.item " +
   			"HAVING COUNT(DISTINCT Player.AccountNo) >= 10"; 

		System.out.println("List the name of weapons that is used by at least 10 Players.");
		executeQuery(query);
	}

	// Driver
	public static void main(String[] args) {
		DatabaseLibrary databaseLibrary = new DatabaseLibrary("coursework"); // passing the database name
        databaseLibrary.run();

		databaseLibrary.query1();
		databaseLibrary.query2();
		databaseLibrary.query3();
		databaseLibrary.query4();
		databaseLibrary.query5();
    }
}

// java -classpath ".:sqlite-jdbc-3.20.1.jar" .java
// pragma table_info()
// CREATE TABLE
// INSERT INTO ___ VALUES()
// DROP TABLE
// DELETE FROM ____ WHERE CONDITION
// pragma foreign_keys = ON;


//// Query 1 (Character Attack Score) (What is considered as an attack {hit, miss, victory, Parry}) (Do I need to print only the character name or all the character details) ////
// SELECT oc.Name, COUNT(*) as num_successful_attacks
// FROM Owns_Character oc
// INNER JOIN char_combat_status ccs ON oc.Name = ccs.Name
// WHERE ccs.result = 'Attack'
// GROUP BY oc.Name
// ORDER BY num_successful_attacks DESC
// LIMIT 5;

// SELECT ccs.Name, COUNT(*) as num_successful_attacks
// FROM char_combat_status ccs
// WHERE ccs.result = 'Attack'
// GROUP by ccs.Name
// ORDER BY num_successful_attacks DESC
// LIMIT 5;


//// Query 2 (Character Attack Score) ////
// SELECT oc.Name, COUNT(*) as num_successful_attacks
// FROM Owns_Character oc
// INNER JOIN char_combat_status ccs ON oc.Name = ccs.Name
// WHERE ccs.result = 'Hit'
// GROUP BY oc.Name
// HAVING num_successful_attacks > 5;

// SELECT ccs.Name, COUNT(*) as num_successful_attacks
// FROM char_combat_status ccs
// WHERE ccs.result = 'Hit'
// GROUP by ccs.Name
// HAVING num_successful_attacks > 5;


//// Query 3 ////
// SELECT ccs.Name, COUNT(*) as num_successful_attacks
// FROM char_combat_status ccs
// WHERE ccs.result = 'Hit'
// GROUP by ccs.Name
// ORDER BY num_successful_attacks DESC;


//// Query 4 ////
// select Player.Forename, Player.Surname, COUNT(*)
// FROM Player
// INNER JOIN Owns_Character ON Player.AccountNo = Owns_Character.AccountNo
// GROUP BY Player.AccountNo
// HAVING COUNT(*) >= 2;


//// Query 5 (Not Sure if it is Character/Player) ////
// SELECT wi.item, COUNT(*) AS num_players
// FROM weapon_inv wi
// INNER JOIN Weapon w ON w.item = wi.item
// GROUP BY w.item
// HAVING num_players >= 10

// SELECT w.item, COUNT(*) AS num_players
// FROM Weapon w
// INNER JOIN weapon_inv wi ON w.item = wi.item
// GROUP BY w.item
// HAVING num_players >= 10

// SELECT wi.item, COUNT(*) AS num_players
// FROM weapon_inv wi
// GROUP BY wi.item
// HAVING num_players >= 10;

//// Using the Player /////
// select wi.item, COUNT(Player.AccountNo) AS num_players
// FROM weapon_inv wi
// INNER JOIN
// (Owns_Character INNER JOIN Player ON Owns_Character.ACcountNo = Player.AccountNo)
// ON Owns_Character.Name = wi.Name
// GROUP BY wi.item
// HAVING COUNT(DISTINCT Player.AccountNo) >= 2 