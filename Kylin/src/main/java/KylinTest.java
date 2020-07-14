import java.sql.*;

/**
 * @author lucas
 * @create 2020-07-12-20:41
 */
public class KylinTest {
    public static void main(String[] args) {
        Connection connection = null;
        try {
            // 定义驱动
            String KylinDriver = "org.apache.kylin.jdbc.Driver";
            // 定义url
            String KylinUrl = "jdbc:kylin://master:7070/test";
            // 定义user
            String KylinUser = "ADMIN";
            // 定义password
            String KylinPasswd = "KYLIN";
            // 加载驱动，获取连接
            Class.forName(KylinDriver);
            connection = DriverManager.getConnection(KylinUrl, KylinUser, KylinPasswd);
            // 编写sql
            String sql = "select e.job,sum(e.sal) as sum_sal from emp e join dept d on e.deptno=d.deptno group by e.job";
            PreparedStatement ps = connection.prepareStatement(sql);
            // 执行查询
            ResultSet resultSet = ps.executeQuery();
            // 打印结果
            while (resultSet.next()) {
                System.out.println(resultSet.getString(1) + "\t" + resultSet.getDouble(2));
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            try {
                connection.close();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }


    }
}
