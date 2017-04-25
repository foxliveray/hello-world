package jdbc;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


/**
 * @author 钱洋
 * @date 2017年4月21日 下午2:43:23
 */
public class Main {
	public void select() {
		String sql = "SELECT first_name, last_name, email FROM candidates";

		try (Connection conn = MySQLJDBCUtil.getConnection();
				Statement stmt = conn.createStatement();
				ResultSet rs = stmt.executeQuery(sql)) {

			// 循环整个结果集
			while (rs.next()) {
				System.out.println(
						rs.getString("first_name") + "\t" + rs.getString("last_name") + "\t" + rs.getString("email"));
			}
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
	}

	public void update() {
		String sqlUpdate = "UPDATE candidates SET last_name = ? WHERE id = ?";
		try (Connection conn = MySQLJDBCUtil.getConnection();
				PreparedStatement pstmt = conn.prepareStatement(sqlUpdate)) {

			// 准备更新的数据
			String lastName = "William";
			int id = 100;
			pstmt.setString(1, lastName);
			pstmt.setInt(2, id);
			int rowAffected = pstmt.executeUpdate();
			System.out.println(String.format("Row affected %d", rowAffected));

			// 重新使用prepared statement
			lastName = "Grohe";
			id = 101;
			pstmt.setString(1, lastName);
			pstmt.setInt(2, id);
			rowAffected = pstmt.executeUpdate();
			System.out.println(String.format("Row affected %d", rowAffected));
		} catch (SQLException ex) {
			System.out.println(ex.getMessage());
		}
	}

	public static int insertCandidate(String firstname, String lastname, Date dob, String email, String phone) {
		// 插入一个新的成员
		ResultSet rs = null;
		int candidateId = 0;

		String sql = "INSERT INTO candidates(first_name,last_name,dob,phone,email) values(?,?,?,?,?)";
		try (Connection conn = MySQLJDBCUtil.getConnection();
				PreparedStatement pstmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);) {
			// 给statement设置变量
			pstmt.setString(1, firstname);
			pstmt.setString(2, lastname);
			pstmt.setDate(3, dob);
			pstmt.setString(4, phone);
			pstmt.setString(5, email);

			int rowAffected = pstmt.executeUpdate();
			if (rowAffected == 1) {
				// 获得成员id
				rs = pstmt.getGeneratedKeys();
				if (rs.next())
					candidateId = rs.getInt(1);
			}
		} catch (SQLException ex) {
			System.out.println(ex.getMessage());
		} finally {
			try {
				if (rs != null)
					rs.close();
			} catch (SQLException e) {
				System.out.println(e.getMessage());
			}
		}

		return candidateId;
	}

	public static void addCandidate(String firstname,String lastname,Date dob,String email,String phone,int[] skills) {
		Connection conn = null;
		//用于插入一个新的成员
		PreparedStatement pstmt = null;
		//用于给成员分配技能
		PreparedStatement pstmtAssignment = null;
		//用于获得成员的id
		ResultSet rs = null;
		
		try {
			conn = MySQLJDBCUtil.getConnection();
			//关闭自动提交事务
			conn.setAutoCommit(false);
			//插入成员
			String sqlInsert = "INSERT INTO candidates(first_name,last_name,dob,phone,email) values(?,?,?,?,?)";
			
			pstmt = conn.prepareStatement(sqlInsert,Statement.RETURN_GENERATED_KEYS);
			pstmt.setString(1, firstname);
			pstmt.setString(2, lastname);
			pstmt.setDate(3, dob);
			pstmt.setString(4, phone);
			pstmt.setString(5, email);
			
			int rowAffected = pstmt.executeUpdate();
			rs = pstmt.getGeneratedKeys();
			int candidateId = 0;
			if (rs.next())
				candidateId = rs.getInt(1);
			
			//如果插入成员成功了，分配技能给成员
			if (rowAffected == 1){
				String sqlPivot = "INSERT INTO candidate_skills(candidate_id,skill_id) values(?,?)";
				pstmtAssignment = conn.prepareStatement(sqlPivot);
				for (int skillId : skills) {
					pstmtAssignment.setInt(1, candidateId);
					pstmtAssignment.setInt(2, skillId);
					pstmtAssignment.executeUpdate();
				}
				conn.commit();
				System.out.println("The transaction has been committed.");
			}else {
				conn.rollback();
			}
		}catch (SQLException ex) {
			//返回事务
			try {
				if(conn != null)
					conn.rollback();
			}catch (SQLException e) {
				System.out.println(e.getMessage());
			}
			
			System.out.println(ex.getMessage());
		}finally {
			try {
				if (rs != null)
					rs.close();
				if (pstmt != null)
					pstmt.close();
				if (pstmtAssignment != null)
					pstmtAssignment.close();
				if (conn != null)
					conn.close();
			}catch (SQLException e) {
				System.out.println(e.getMessage());
			}
		}
	}
	
	public static void getSkills(int candidateId) {
		String query = "{call get_candidate_skill(?)}";
		ResultSet rs;
		
		try(Connection conn = MySQLJDBCUtil.getConnection();
			CallableStatement stmt = conn.prepareCall(query)) {
			
			stmt.setInt(1, candidateId);
			rs = stmt.executeQuery();
			while (rs.next()){
				System.out.println(String.format("%s - %s", rs.getString("first_name")+" "+rs.getString("last_name"),rs.getString("skill")));
			}
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
	}
	
	public static void writeBlob(int candidateId,String filename){
		//更新语句
		String updateSQL = "UPDATE candidates set resume = ? where id = ?";
		try(Connection conn = MySQLJDBCUtil.getConnection();
			PreparedStatement pstmt = conn.prepareStatement(updateSQL)){
			
			//读取文件
			File file = new File(filename);
			FileInputStream input = new FileInputStream(file);
			
			//设置参数
			pstmt.setBinaryStream(1, input);
			pstmt.setInt(2, candidateId);
			
			//存储resume文件到数据库中
			System.out.println("Reading file " + file.getAbsolutePath());
			System.out.println("Store file in the database.");
			pstmt.executeUpdate();
			
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		} catch (FileNotFoundException ex) {
			System.out.println(ex.getMessage());
		}
	}
	
	public static void readBlob(int candidateId,String filename){
		//更新语句
		String selectSQL = "SELECT resume from candidates where id = ?";
		ResultSet rs = null;
		
		try(Connection conn = MySQLJDBCUtil.getConnection();
			PreparedStatement pstmt = conn.prepareStatement(selectSQL);){
			
			//设置参数
			pstmt.setInt(1, candidateId);
			rs = pstmt.executeQuery();
			
			//把二进制流写入文件
			File file = new File(filename);
			FileOutputStream output = new FileOutputStream(file);
			System.out.println("Writing to file " + file.getAbsolutePath());
			
			while (rs.next()){
				InputStream input = rs.getBinaryStream("resume");
				byte[] buffer = new byte[1024];
				while (input.read(buffer) > 0){
					output.write(buffer);
				}
			}
		} catch (SQLException | IOException e) {
			System.out.println(e.getMessage());
		} finally {
			try {
				if (rs != null){
					rs.close();
				}
			}catch (SQLException e) {
				System.out.println(e.getMessage());
			}
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		Main m = new Main();
		m.select();
		m.update();
		
		int id = insertCandidate("Bush", "Lily", Date.valueOf("1980-01-04"),"bush.l@yahoo.com", "(408)898-6666");
		System.out.println(String.format("A new candidate with id %d has beeninserted.", id));
		
		int[] skills = {1,2,3};
		addCandidate("John", "Doe", Date.valueOf("1990-01-04"), "john.d@yahoo.com", "(408)898-5641", skills);
		
		getSkills(135);
		
		writeBlob(135, "C://Users//abc//Desktop//johndoe_resume.pdf");
		readBlob(135, "johndoe_resume_from_db.pdf");
	}

}
