package HbaseAPI;

import org.junit.Test;

/**
 * @author lucas
 * @create 2020-07-09-16:28
 */
public class testApi {
    /**
     * 判断HBase中的表是否存在
     */
    @Test
    public void testExists() {
        boolean exist = HBaseApi.isTableExist("student");
        System.out.println(exist);
    }

    @Test
    public void testCreateTable() {
        HBaseApi.createTable("emp", "info", "extend");
    }

    @Test
    public void testDelete() {
        HBaseApi.deleteTable("emp");
        System.out.println("------------------");
        HBaseApi.deleteTable("dept");

    }

    @Test
    public void testaddDataRow() {
        HBaseApi.addRowData("student","1004","info","name","jack");
        HBaseApi.addRowData("student", "1004", "info", "age", "22");
        HBaseApi.addRowData("student", "1005", "info", "sex", "male");
    }

    @Test
    public void testDeleteMultiRow() {
        HBaseApi.deleteMultiRow("student","1004","1005");
    }

    @Test
    public void testGetAllRows() {
        HBaseApi.getAllRows("student");
    }

    @Test
    public void testGetRow() {
        HBaseApi.getRow("student","1002");
    }

    @Test
    public void testGetRowQualifier() {
        HBaseApi.getRowQualifier("student","1002","info","name");
        HBaseApi.getRowQualifier("student","1002","info","age");
        HBaseApi.getRowQualifier("student","1002","info","sex");
    }

}
