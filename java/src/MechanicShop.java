/*
 * Template JAVA User Interface
 * =============================
 *
 * Database Management Systems
 * Department of Computer Science &amp; Engineering
 * University of California - Riverside
 *
 * Target DBMS: 'Postgres'
 *
 */


import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.ArrayList;
import java.util.Random; 

/**
 * This class defines a simple embedded SQL utility class that is designed to
 * work with PostgreSQL JDBC drivers.
 *
 */

public class MechanicShop{
	//reference to physical database connection
	private Connection _connection = null;
	static BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
	
	public MechanicShop(String dbname, String dbport, String user, String passwd) throws SQLException {
		System.out.print("Connecting to database...");
		try{
			// constructs the connection URL
			String url = "jdbc:postgresql://localhost:" + dbport + "/" + dbname;
			System.out.println ("Connection URL: " + url + "\n");
			
			// obtain a physical connection
	        this._connection = DriverManager.getConnection(url, user, passwd);
	        System.out.println("Done");
		}catch(Exception e){
			System.err.println("Error - Unable to Connect to Database: " + e.getMessage());
	        System.out.println("Make sure you started postgres on this machine");
	        System.exit(-1);
		}
	}
	
	/**
	 * Method to execute an update SQL statement.  Update SQL instructions
	 * includes CREATE, INSERT, UPDATE, DELETE, and DROP.
	 * 
	 * @param sql the input SQL string
	 * @throws java.sql.SQLException when update failed
	 * */
	public void executeUpdate (String sql) throws SQLException { 
		// creates a statement object
		Statement stmt = this._connection.createStatement ();

		// issues the update instruction
		stmt.executeUpdate (sql);

		// close the instruction
	    stmt.close ();
	}//end executeUpdate

	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and outputs the results to
	 * standard out.
	 * 
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQueryAndPrintResult (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		/*
		 *  obtains the metadata object for the returned result set.  The metadata
		 *  contains row and column info.
		 */
		ResultSetMetaData rsmd = rs.getMetaData ();
		int numCol = rsmd.getColumnCount();
		int rowCount = 0;
		
		//iterates through the result set and output them to standard out.
		boolean outputHeader = true;
		while (rs.next()){
			if(outputHeader){
				for(int i = 1; i <= numCol; i++){
					System.out.print(rsmd.getColumnName(i) + "\t");
			    }
			    System.out.println();
			    outputHeader = false;
			}
			for (int i=1; i<=numCol; ++i)
				System.out.print (rs.getString (i) + "\t");
			System.out.println ();
			++rowCount;
		}//end while
		stmt.close ();
		return rowCount;
	}
	
	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the results as
	 * a list of records. Each record in turn is a list of attribute values
	 * 
	 * @param query the input query string
	 * @return the query result as a list of records
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public List<List<String>> executeQueryAndReturnResult (String query) throws SQLException { 
		//creates a statement object 
		Statement stmt = this._connection.createStatement (); 
		
		//issues the query instruction 
		ResultSet rs = stmt.executeQuery (query); 
	 
		/*
		 * obtains the metadata object for the returned result set.  The metadata 
		 * contains row and column info. 
		*/ 
		ResultSetMetaData rsmd = rs.getMetaData (); 
		int numCol = rsmd.getColumnCount (); 
		int rowCount = 0; 
	 
		//iterates through the result set and saves the data returned by the query. 
		boolean outputHeader = false;
		List<List<String>> result  = new ArrayList<List<String>>(); 
		while (rs.next()){
			List<String> record = new ArrayList<String>(); 
			for (int i=1; i<=numCol; ++i) 
				record.add(rs.getString (i)); 
			result.add(record); 
		}//end while 
		stmt.close (); 
		return result; 
	}//end executeQueryAndReturnResult
	
	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the number of results
	 * 
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQuery (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		int rowCount = 0;

		//iterates through the result set and count nuber of results.
		while(rs.next()){
			rowCount++;
		}//end while
		stmt.close ();
		return rowCount;
	}
	
	/**
	 * Method to fetch the last value from sequence. This
	 * method issues the query to the DBMS and returns the current 
	 * value of sequence used for autogenerated keys
	 * 
	 * @param sequence name of the DB sequence
	 * @return current value of a sequence
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	
	public int getCurrSeqVal(String sequence) throws SQLException {
		Statement stmt = this._connection.createStatement ();
		
		ResultSet rs = stmt.executeQuery (String.format("Select currval('%s')", sequence));
		if (rs.next()) return rs.getInt(1);
		return -1;
	}

	/**
	 * Method to close the physical connection if it is open.
	 */
	public void cleanup(){
		try{
			if (this._connection != null){
				this._connection.close ();
			}//end if
		}catch (SQLException e){
	         // ignored.
		}//end try
	}//end cleanup

	/**
	 * The main execution method
	 * 
	 * @param args the command line arguments this inclues the <mysql|pgsql> <login file>
	 */
	public static void main (String[] args) {
		if (args.length != 3) {
			System.err.println (
				"Usage: " + "java [-classpath <classpath>] " + MechanicShop.class.getName () +
		            " <dbname> <port> <user>");
			return;
		}//end if
		
		MechanicShop esql = null;
		
		try{
			System.out.println("(1)");
			
			try {
				Class.forName("org.postgresql.Driver");
			}catch(Exception e){

				System.out.println("Where is your PostgreSQL JDBC Driver? " + "Include in your library path!");
				e.printStackTrace();
				return;
			}
			
			System.out.println("(2)");
			String dbname = args[0];
			String dbport = args[1];
			String user = args[2];
			
			esql = new MechanicShop (dbname, dbport, user, "");
			
			boolean keepon = true;
			while(keepon){
				System.out.println("MAIN MENU");
				System.out.println("---------");
				System.out.println("1. AddCustomer");
				System.out.println("2. AddMechanic");
				System.out.println("3. AddCar");
				System.out.println("4. InsertServiceRequest");
				System.out.println("5. CloseServiceRequest");
				System.out.println("6. ListCustomersWithBillLessThan100");
				System.out.println("7. ListCustomersWithMoreThan20Cars");
				System.out.println("8. ListCarsBefore1995With50000Milles");
				System.out.println("9. ListKCarsWithTheMostServices");
				System.out.println("10. ListCustomersInDescendingOrderOfTheirTotalBill");
				System.out.println("11. < EXIT");
				
				/*
				 * FOLLOW THE SPECIFICATION IN THE PROJECT DESCRIPTION
				 */
				switch (readChoice()){
					case 1: AddCustomer(esql); break;
					case 2: AddMechanic(esql); break;
					case 3: AddCar(esql); break;
					case 4: InsertServiceRequest(esql); break;
					case 5: CloseServiceRequest(esql); break;
					case 6: ListCustomersWithBillLessThan100(esql); break;
					case 7: ListCustomersWithMoreThan20Cars(esql); break;
					case 8: ListCarsBefore1995With50000Milles(esql); break;
					case 9: ListKCarsWithTheMostServices(esql); break;
					case 10: ListCustomersInDescendingOrderOfTheirTotalBill(esql); break;
					case 11: keepon = false; break;
				}
			}
		}catch(Exception e){
			System.err.println (e.getMessage ());
		}finally{
			try{
				if(esql != null) {
					System.out.print("Disconnecting from database...");
					esql.cleanup ();
					System.out.println("Done\n\nBye !");
				}//end if				
			}catch(Exception e){
				// ignored.
			}
		}
	}

	public static int readChoice() {
		int input;
		// returns only if a correct value is given.
		do {
			System.out.print("Please make your choice: ");
			try { // read the integer, parse it and break.
				input = Integer.parseInt(in.readLine());
				break;
			}catch (Exception e) {
				System.out.println("Your input is invalid!");
				continue;
			}//end try
		}while (true);
		return input;
	}//end readChoice
	

/* example-----------------------------------------------------

try{
		String query = "SELECT * FROM customer;";

		// System.out.print("\tEnter cost: $");
		// String input = in.readLine();
		// query += input;

		int rowCount = esql.executeQuery(query);
		System.out.println ("total row(s): " + rowCount);
        
	    }catch(Exception e)
	    {
		System.err.println (e.getMessage ());
		}		

--------------------------------------------------------------*/

	public static void AddCustomer(MechanicShop esql){//1
	    try{
			int idIn  = Integer.parseInt(esql.executeQueryAndReturnResult("SELECT MAX(id) FROM customer;").get(0).get(0)) + 1;
			System.out.print("\tnext id: " + idIn +"\n");
			System.out.print("\tEnter fname: $");
			String fnameIn = in.readLine();
			System.out.print("\tEnter lname: $");
			String lnameIn = in.readLine();
			System.out.print("\tEnter phone ie. (###)###-####: $");
			String phoneIn = in.readLine();
			System.out.print("\tEnter address ie. street city: $");
			String addressIn = in.readLine();

			String sql = "INSERT INTO customer VALUES ('" + idIn + "', '" + fnameIn + "', '" + lnameIn + "', '" + phoneIn + "', '" + addressIn + "');";
	
			esql.executeUpdate(sql);

			
			
			}catch(Exception e)
			{
			System.err.println (e.getMessage ());
			}		
	}
	
	public static void AddMechanic(MechanicShop esql){//2
		try{
			int idIn  = Integer.parseInt(esql.executeQueryAndReturnResult("SELECT MAX(id) FROM mechanic;").get(0).get(0)) + 1;
			System.out.print("\tnext id: " + idIn +"\n");
			System.out.print("\tEnter fname: $");
			String fnameIn = in.readLine();
			System.out.print("\tEnter lname: $");
			String lnameIn = in.readLine();
			System.out.print("\tEnter experience ie. #: $");
			String experienceIn = in.readLine();

			String sql = "INSERT INTO mechanic VALUES ('" + idIn + "', '" + fnameIn + "', '" + lnameIn + "', '" + experienceIn +  "');";
	
			esql.executeUpdate(sql);

			
			
			}catch(Exception e)
			{
			System.err.println (e.getMessage ());
			}		
	}
	
	public static void AddCar(MechanicShop esql){//3
		try{
			
			String vinIn = "";
			boolean match = true;
			while(match) {
				//generate a random string of uppercase letters of length 6
				int leftLimit = 65; // letter 'A'
    			int rightLimit = 90; // letter 'Z'
    			int targetStringLength = 6;
    			Random random = new Random();
    			StringBuilder buffer = new StringBuilder(targetStringLength);
    			for (int i = 0; i < targetStringLength; i++) {
        			int randomLimitedInt = leftLimit + (int) 
          			(random.nextFloat() * (rightLimit - leftLimit + 1));
        			buffer.append((char) randomLimitedInt);
    			}
    			String letters = buffer.toString();
 
				//geterate random string of numbers of length 10
				leftLimit = 49; // letter '1'
    			rightLimit = 57; // letter '9'
    			targetStringLength = 10;
    			random = new Random();
    			buffer = new StringBuilder(targetStringLength);
    			for (int i = 0; i < targetStringLength; i++) {
        			int randomLimitedInt = leftLimit + (int) 
          			(random.nextFloat() * (rightLimit - leftLimit + 1));
					buffer.append((char) randomLimitedInt);
					if(i == 0)
						leftLimit--; // cannot start with 0
    			}
				String numbers = buffer.toString();
				
				//concatonate them so that first comes the letters then the numbers
				vinIn = letters + numbers;


				String query = "SELECT * FROM car WHERE vin = '" + vinIn + "';";
				int rowCount = esql.executeQuery(query);
				if(rowCount == 0)
					match = false;
			}
			int ownership_idIn  = Integer.parseInt(esql.executeQueryAndReturnResult("SELECT MAX(ownership_id) FROM owns;").get(0).get(0)) + 1;
			System.out.print("\tnext ownership_id: $" + ownership_idIn +"\n");
			System.out.print("\tnext vin: $" + vinIn +"\n");
			System.out.print("\tEnter customer_id: $");
			String customer_idIn = in.readLine();
			System.out.print("\tEnter make: $");
			String makeIn = in.readLine();
			System.out.print("\tEnter model: $");
			String modelIn = in.readLine();
			System.out.print("\tEnter year ie. ####: $");
			String yearIn = in.readLine();

			String checkQuery1 = "SELECT * FROM customer WHERE id = '" + customer_idIn + "';";
			int rowCount1 = esql.executeQuery(checkQuery1);

			if(rowCount1 == 1) {
				String sql = "INSERT INTO car VALUES ('" + vinIn + "', '" + makeIn + "', '" + modelIn + "', '" + yearIn +  "');";
				esql.executeUpdate(sql);

				sql = "INSERT INTO owns VALUES ('" + ownership_idIn + "', '" + customer_idIn + "', '" + vinIn + "');";
				esql.executeUpdate(sql);
			}
			else {
				System.out.print("\tError (customer_id)\n");
			}

			

			
			
			}catch(Exception e)
			{
			System.err.println (e.getMessage ());
			}		
	}
	
	public static void InsertServiceRequest(MechanicShop esql){//4
		try{
			int ridIn  = Integer.parseInt(esql.executeQueryAndReturnResult("SELECT MAX(rid) FROM service_request;").get(0).get(0)) + 1;
			System.out.print("\tnext rid: " + ridIn +"\n");
			System.out.print("\tEnter customer_id: $");
			String customer_idIn = in.readLine();
			System.out.print("\tcar_vin: $");
			String car_vinIn = in.readLine();
			System.out.print("\tEnter date ie. yyyy-mm-dd: $");
			String dateIn = in.readLine();
			System.out.print("\tEnter odometer: $");
			String odometerIn = in.readLine();
			System.out.print("\tEnter complain: $");
			String complainIn = in.readLine();

			String checkQuery1 = "SELECT * FROM customer WHERE id = '" + customer_idIn + "';";
			int rowCount1 = esql.executeQuery(checkQuery1);
			String checkQuery2 = "SELECT * FROM car WHERE vin = '" + car_vinIn + "';";
			int rowCount2 = esql.executeQuery(checkQuery2);

			if(rowCount1 == 1){
				if(rowCount2 == 1) {
					String sql = "INSERT INTO service_request VALUES ('" + 
					ridIn + "', '" + customer_idIn + "', '" + car_vinIn + "', '" + dateIn +  "', '" + odometerIn + "', '" + complainIn + "');";
					//System.out.print(sql+"\n");
					esql.executeUpdate(sql);
				}
				else
					System.out.print("\tError (car_vin)\n");
			} 
			else
				System.out.print("\tError (customer_id)\n");

			

			
			
			}catch(Exception e)
			{
			System.err.println (e.getMessage ());
			}		
	}
	
	public static void CloseServiceRequest(MechanicShop esql) throws Exception{//5
		try{
			System.out.print("\tEnter rid: $");
			String ridIn = in.readLine();
			System.out.print("\tEnter mid: $");
			String midIn = in.readLine();
			System.out.print("\tEnter date ie. yyyy-mm-dd: $");
			String dateIn = in.readLine();
			System.out.print("\tEnter comment: $");
			String commentIn = in.readLine();
			System.out.print("\tEnter bill: $");
			String billIn = in.readLine();

			String checkQuery1 = "SELECT * FROM service_request WHERE rid = '" + ridIn + "';";
			int rowCount1 = esql.executeQuery(checkQuery1);
			String checkQuery2 = "SELECT * FROM closed_request WHERE wid = '" + ridIn + "';";
			int rowCount2 = esql.executeQuery(checkQuery2);
			String checkQuery3 = "SELECT * FROM mechanic WHERE id = '" + midIn + "';";
			int rowCount3 = esql.executeQuery(checkQuery3);

			if(rowCount1 == 1){
				if(rowCount2 == 0) {
					if(rowCount3 == 1) {
						String sql = "INSERT INTO closed_request VALUES ('" + 
						ridIn + "', '" + ridIn + "', '" + midIn + "', '" + dateIn  + "', '" + commentIn + "', '" + billIn + "');";
						esql.executeUpdate(sql);
					}
					else
						System.out.print("\tError (mechanic doesn't exist)\n");
				}
				else
					System.out.print("\tError (service_request was already closed)\n");
			} 
			else
				System.out.print("\tError (service_request doesn't exist)\n");

			

			
			
			}catch(Exception e)
			{
			System.err.println (e.getMessage ());
			}		
	}
	
	public static void ListCustomersWithBillLessThan100(MechanicShop esql){//6
		try{
			String query = "SELECT * FROM closed_request WHERE bill < 100;";
	
			List<List<String>> result  = esql.executeQueryAndReturnResult(query);
			printResult(result);
			
		}catch(Exception e) {
			System.err.println (e.getMessage ());
		}		
	}
	
	public static void ListCustomersWithMoreThan20Cars(MechanicShop esql){//7
		try{
			String query = "SELECT * FROM customer, (SELECT customer_id, COUNT(customer_id) FROM owns GROUP BY customer_id HAVING COUNT(customer_id) > 20 ) B WHERE customer.id = B.customer_id ORDER BY COUNT;";
	
			List<List<String>> result  = esql.executeQueryAndReturnResult(query);
			printResult(result);
			
		}catch(Exception e) {
			System.err.println (e.getMessage ());
		}		
	}
	
	public static void ListCarsBefore1995With50000Milles(MechanicShop esql){//8
		try{
			String query = "SELECT * FROM car, service_request WHERE car.year<'1995' AND service_request.odometer >='50000' AND car.vin = service_request.car_vin;";
	
			List<List<String>> result  = esql.executeQueryAndReturnResult(query);
			printResult(result);
			
		}catch(Exception e) {
			System.err.println (e.getMessage ());
		}	
	}
	
	public static void ListKCarsWithTheMostServices(MechanicShop esql){//9
		try{
		 System.out.print("How many cars with the most services would you like to see listed?");
		 String input = in.readLine();
		 int kinput = Integer.parseInt(input);
			String query = "SELECT car.make, car.model, count(service_request) from car, service_request where car.vin = car_vin group by car.vin order by count desc limit " + kinput + ";";
	
			List<List<String>> result  = esql.executeQueryAndReturnResult(query);
			printResult(result);
			
		}catch(Exception e) {
			System.err.println (e.getMessage ());
		}		
		
	}
	
	public static void ListCustomersInDescendingOrderOfTheirTotalBill(MechanicShop esql){//10
		try{
			String query = "SELECT id, fname, lname, SUM(bill) FROM customer, service_request, closed_request WHERE customer.id = service_request.customer_id AND service_request.rid = closed_request.rid GROUP BY id ORDER BY SUM(bill) DESC;";
	
			List<List<String>> result  = esql.executeQueryAndReturnResult(query);
			printResult(result);
			
		}catch(Exception e) {
			System.err.println (e.getMessage ());
		}		
		
	}


	public static void printResult(List<List<String>> result) {
		int n = result.size();
		for(int i = 0; i < n; i++) {
			System.out.println (result.get(i) + "\n");
		}
	} 
	
}


	