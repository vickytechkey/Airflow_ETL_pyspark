class mysql_connection:
    def credentials(self):
        return {
            "user":"root",
            "password":"vtechnosoft@123A",
            "driver":"com.mysql.cj.jdbc.Driver"
        }
    
    def mysqlurl(self , database_name):
        return f"jdbc:mysql://localhost:3306/{database_name}"
    

