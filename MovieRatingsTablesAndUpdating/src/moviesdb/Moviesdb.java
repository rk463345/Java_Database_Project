package moviesdb;

//Ryan Karpinski
//11/19/2018
//Assignment6

import java.io.IOError;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;


public class Moviesdb 
{
	public static void main()
	{
		Connection con = null;
		Statement state = null;
		List<String[]> data = new ArrayList<String[]>();
		String filename = "movies.dat";
		String filename1 = "ratings.dat";
		String filename2 = "users.dat";
		data = read_dat(filename);
		try
		{
			Class.forName("oracle.jdbc.OracleDriver");
		}
		catch(ClassNotFoundException e)
		{
			System.out.println("Class not found\n" + e);
			System.exit(1);
		}
		try
		{
		con = DriverManager.getConnection("", "", "");
		state = con.createStatement();
		}
		catch(SQLException se)
		{
			System.out.println(se);
			System.exit(1);
		}
		try
		{
			create_tables(state);
		}
		catch(Exception e)
		{
			System.out.println(e);
		}
		finally
		{
			create_temp_tables(state);
			System.out.print("Rows Added to Movies Table: ");
			data = read_dat(filename);
			for (int i=0; i<data.size(); i++)
				insert_row_movies(data.get(i), state);
			System.out.print("Rows Added to Ratings Table: ");
			data = read_dat(filename1);
			for (int i=0; i<data.size(); i++)
				insert_row_rating(data.get(i), state);
			System.out.print("Rows Added to Users Table: ");
			data = read_dat(filename2);
			for (int i=0; i<data.size(); i++)
				insert_row_user(data.get(i), state);
			populate_age(state);
			populate_occupation(state);
			merge_tables(state);
			query(state);
		}
	}
	private static List<String[]> read_dat(String filename)
	{
		int row_count = 0;
		List<String[]> data = new ArrayList<String[]>();
		try
		{
			Scanner fileScan = new Scanner(filename);
			while(fileScan.hasNext())
			{
				String row = fileScan.next().toString();
				String[] attribs = row.split("::");
				attribs[1].replace('(', ' ').replace(')', ' ');
				attribs[1].split(" ");
				System.out.println(attribs.toString());
				data.add(attribs);
				row_count += 1;
			}
			fileScan.close();
		}
		catch(IOError e)
		{
			System.out.println(e);
			System.exit(1);
		}
		System.out.println(row_count);
		return data;
	}
	private static void insert_row_movies(String[] row, Statement state)
	{
		String sqlStatment = null;
		sqlStatment = "INSERT INTO tempmovies VALUES (" + row[0] + ", "
														+ row[1] + ", "
														+ row[2] + ")";
		insert_row_movie_cat(row[0], row[3], state);
		try
		{
			state.executeUpdate(sqlStatment);
		}
		catch (SQLException se)
		{
			System.exit(1);
		}
	}
	private static void insert_row_user(String[] row, Statement state)
	{
		String sqlStatement = null;
		sqlStatement = "INSERT INTO tempmovies VALUES (" + row[0] + ", "
														+ row[1] + ", "
														+ row[2] + ", " 
														+ row[3] + ", "
														+ row[4] + ")";
		try
		{
			state.executeUpdate(sqlStatement);
		}
		catch (SQLException se)
		{
			System.exit(1);
		}
	}
	private static void insert_row_movie_cat(String attrib1, String attrib2, Statement state)
	{
		String sqlStatment = null;
		sqlStatment = "INSERT INTO temp_movie_cat VALUES (" + attrib1 + ", "
															+ attrib2 + ")";
	try
	{
		state.executeUpdate(sqlStatment);
	}
	catch (SQLException se)
	{
		System.exit(1);
	}
	}
	private static void insert_row_rating(String[] row, Statement state)
	{
		String sqlStatment = null;
		sqlStatment = "INSERT INTO tempratings VALUES (" + row[0] + ", "
														+ row[1] + ", "
														+ row[2] + ", " 
														+ row[3] + ")";
	try
	{
		state.executeUpdate(sqlStatment);
	}
	catch (SQLException se)
	{
		System.exit(1);
	}
	}
	private static void create_temp_tables(Statement state)
	{
		String sqlStatement = "CREATE TABLE TEMPMOVIES ("
             			+ "MOVIEID 	NUMBER(9) PRIMARY KEY,"
             			+ "Title   	VARCHAR2(20)"
             			+ "YEAR		VARCHAR2(4))";
		String sqlStatement1 = "CREATE TABLE TEMPUSER ("
                         + "USERID NUMBER(9) PRIMARY KEY,"
                         + " GENDER CHAR(1),"
                         + " AGECODE NUMBER (2) FOREIGN KEY REFERENCES TEMPAGE(AGEID),"
                         + " OCCUPATION NUMBER(2) FOREIGN KEY REFERENCES TEMPOCCUPATION(OCCUPATIONID),"
                         + " ZIPCODE NUMBER(15))";
		String sqlStatement2 = "CREATE TABLE TEMPRATINGS ("
             			+ "USERID NUMBER(9) FOREIGN KEY REFERENCES TEMPUSER(USERID),"
             			+ "MOVIEID VARCHAR(20) FOREIGN KEY REFERENCES TEMPMOVIES(MOVIEID,"
             			+ "RATING NUMBER(2)),"
             			+ "TIMESTAMP   DATE())";
		String sqlStatement3 = "CREATE TABLE TEMPAGE ("
             			+ "AGEID   VARCHAR2(2) PRIMARY KEY,"
             			+ "AGERANGE VARCHAR2(20))";
		String sqlStatement4 = "CREATE TABLE TEMPOCCUPATION ("
             			+ "OCCUPATIONID VARCHAR2(2) PRIMARY KEY,"
             			+ "OCCUPATION VARCHAR(20))";
		String sqlStatement5 = "CREATE TABLE TEMP_MOVIE_CAT ("
							+ "MOVIEID NUMBER(9) FOREIGN KEY REFERENCES TEMPMOVIES(MOVIEID), "
							+ "CATEGORY VARCHAR2(50))";
		try
		{
			state.executeUpdate(sqlStatement);
			state.executeUpdate(sqlStatement1);
			state.executeUpdate(sqlStatement2);
			state.executeUpdate(sqlStatement3);
			state.executeUpdate(sqlStatement4);
			state.executeUpdate(sqlStatement5);
		}
		catch (SQLException se)
		{
			System.exit(1);
		}
	}
	private static void create_tables(Statement state)
	{
		String sqlStatement = "CREATE TABLE MOVIES ("
 						+ "MOVIEID 	NUMBER(9) PRIMARY KEY,"
 						+ "Title   	VARCHAR2(20)"
 						+ "YEAR		VARCHAR2(4))";
		String sqlStatement1 = "CREATE TABLE USER ("
             			+ "USERID NUMBER(9) PRIMARY KEY,"
             			+ " GENDER CHAR(1),"
             			+ " AGECODE NUMBER (2) FOREIGN KEY REFERENCES AGE(AGEID),"
             			+ " OCCUPATION NUMBER(2) FOREIGN KEY REFERENCES OCCUPATION(OCCUPATIONID),"
             			+ " ZIPCODE NUMBER(15))";
		String sqlStatement2 = "CREATE TABLE RATINGS ("
 						+ "USERID NUMBER(9) FOREIGN KEY REFERENCES USER(USERID),"
 						+ "MOVIEID VARCHAR(20) FOREIGN KEY REFERENCES MOVIES(MOVIEID,"
 						+ "RATING NUMBER(2)),"
 						+ "TIMESTAMP   DATE())";
		String sqlStatement3 = "CREATE TABLE AGE ("
 						+ "AGEID   VARCHAR2(2) PRIMARY KEY,"
 						+ "AGERANGE VARCHAR2(20))";
		String sqlStatement4 = "CREATE TABLE OCCUPATION ("
 						+ "OCCUPATIONID VARCHAR2(2) PRIMARY KEY,"
 						+ "OCCUPATION VARCHAR(20))";
		String sqlStatement5 = "CREATE TABLE MOVIE_CAT ("
							+ "MOVIEID NUMBER(9) FOREIGN KEY REFERENCES MOVIES(MOVIEID), "
							+ "CATEGORY VARCHAR2(50))";
		try
		{
			state.executeUpdate(sqlStatement);
			state.executeUpdate(sqlStatement1);
			state.executeUpdate(sqlStatement2);
			state.executeUpdate(sqlStatement3);
			state.executeUpdate(sqlStatement4);
			state.executeUpdate(sqlStatement5);
		}
		catch (SQLException se)
		{
			System.out.println("Tables already exist");
		}
	}
	private static void merge_tables(Statement state)
	{
	    String SQLSTRING1 = "MERGE INTO RATINGS R USING TEMPRATINGS T ON ("
             			+ "R.USERID = T.USERID"
             			+ " AND R.MOVIEID = T.MOVIEID"
             			+ " AND R.RATING = T.RATING"
             			+ "AND R.TIMESTAMP = T.TIMES)"
             			+ "WHEN NOT MATCHED THEN INSERT(R.USERID, R.MOVIEID, R.RATING, R.TIMESTAMP)"
             			+ "VALUES (T.USERID, T.MOVIEID, T.RATING, T.TIMES)";
	    String SQLSTRING2 = "MERGE INTO AGE R USING TEMPAGE T ON ("
             			+ "R.AGEID = T.AGEID"
             			+ " AND R.AGERANGE = T.AGERANGE)"
             			+ " WHEN NOT MATCHED THEN INSERT(R.AGEID, R.AGERANGE)"
             			+ "VALUES (T.AGEID, T.AGERANGE)";
	    String SQLSTRING3 = "MERGE INTO OCCUPATION R USING TEMPOCCUPATION T ON ("
             			+ "R.OCCUPATIONID = T.OCCUPATIONID"
             			+ " AND R.OCCUPATION = T.OCCUPATION)"
             			+ " WHEN NOT MATCHED THEN INSERT(R.OCCUPATIONID, R.OCCUPATION)"
             			+ " VALUES (T.OCCUPATIONID, T.OCCUPATION)";
	    String SQLSTRING4 = "MERGE INTO USERS R USING TEMPUSER T ON ("
             			+ "R.USERID = T.USERID"
             			+ " AND R.GENDER = T.GENDER"
             			+ " AND R.AGECODE = T.AGECODE"
             			+ " AND R.OCCUPATION = T.OCCUPATION"
             			+ " AND R.ZIPCODE = T.ZIPCODE)"
             			+ " WHEN NOT MATCHED THEN INSERT(R.USERID, R.GENDER, R.AGECODE, R.OCCUPATION, R.ZIPCODE)"
             			+ " VALUES (T.USERID, T.GENDER, T.AGECODE, T.OCCUPATION, T.ZIPCODE)";
		try
		{
			state.executeUpdate(SQLSTRING1);
			state.executeUpdate(SQLSTRING2);
			state.executeUpdate(SQLSTRING3);
			state.executeUpdate(SQLSTRING4);
		}
		catch (SQLException se)
		{
			System.exit(1);
		}
	}
	private static void populate_occupation(Statement state)
	{
		String[] codes = {"19:unemployed", "6:doctor/health care",
						"5:customer service", "9:homemaker",
						"15:scientist", "4:college/grad student",
						"13:retired", "14:sales/marketing",
						"7:executive/managerial", "16:self-employed",
						"1:academic/educator", "10:K-12 student",
						"11:lawyer", "8:farmer", "20:writer",
						"3:clerical/admin", "17:technician/engineer",
						"2:artist", "12:programmer", "18:tradesman/craftsman",
						"0:other or not specified"};
		for(int i=0; i<codes.length; i++)
		{
			String[] attribs = codes[i].split(":");
			String sqlStatement = "INSERT INTO tempoccupation ("
									+ attribs[0] + ", " 
									+ attribs[1] + ")";
			try
			{
				state.executeUpdate(sqlStatement);
			}
			catch (SQLException se)
			{
				System.exit(1);
			}
		}
	}
	private static void populate_age(Statement state)
	{
		String[] codes = {"45:45-49", "18:18-24", "35:35-44", 
						"25:25-34", "56:56 and over", 
						"1:Under 19", "50:50-55"};
		for(int i=0; i<codes.length; i++)
		{
			String[] attribs = codes[i].split(":");
			String sqlStatement = "INSERT INTO tempage ("
									+ attribs[0] + ", " 
									+ attribs[1] + ")";
			try
			{
				state.executeUpdate(sqlStatement);
			}
			catch (SQLException se)
			{
				System.exit(1);
			}
		}
	}
	private static void query(Statement state)
	{
		int counting = 1;
		String sqlStatement = "SELECT movie_cat.category, AVG(RATING) AS AVGRATINGS"
							+ "FROM (RATINGS JOIN MOVIES ON RATINGS.MOVIEID = movies.movieid)"
							+ " JOIN movie_cat ON movie_cat.movieid = movies.movieid" 
							+ "GROUP BY movie_cat.category "
							+ "ORDER BY AVGRATINGS;";
		try
		{
			ResultSet search = state.executeQuery(sqlStatement);
			search.afterLast();
			while(counting < 4)
			{
				search.previous();
				String category = search.getString("categories");
				Float avgrating = search.getFloat("avgratings");
				System.out.println(category + "\t\t\t\t" + avgrating);
				counting ++;
			}
		}
		catch(SQLException se)
		{
			System.out.println(se);
			System.exit(1);
		}
	}
}

