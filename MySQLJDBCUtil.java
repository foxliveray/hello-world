package jdbc;

/**
 * @author 钱洋
 * @date 2017年4月19日 下午6:00:15
 */
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class MySQLJDBCUtil {
	public static Connection getConnection() throws SQLException{
		Connection conn = null;
		
		try(FileInputStream f = new FileInputStream("src\\db.properties")){
			//加载properties文件
			Properties pros = new Properties();
			pros.load(f);
			
			//分配数据库参数
			String url =  pros.getProperty("url");
			String user = pros.getProperty("user");
			String password = pros.getProperty("password");
			
			//创建一个数据库连接
			conn = DriverManager.getConnection(url,user,password);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println(e.getMessage());
		}
		return conn;
		
	}
	
	public static void main(String[] args){
		//创建一个来自MySQLJDBCUtil的新连接
		try(Connection conn = MySQLJDBCUtil.getConnection()){
			System.out.println(String.format("Connected to database %s " + "successfully.", conn.getCatalog()));
		} catch (SQLException ex) {
			// TODO Auto-generated catch block
			System.out.println(ex.getMessage());
		}
	}

}
