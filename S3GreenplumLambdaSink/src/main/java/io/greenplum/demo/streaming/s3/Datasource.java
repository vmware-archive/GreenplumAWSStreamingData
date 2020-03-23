/**********************************************************************************************
 Copyright 2020 VMWare Inc
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at
 http://www.apache.org/licenses/LICENSE-2.0
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 *********************************************************************************************/

package io.greenplum.demo.streaming.s3;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.SQLException;

/**
 *
 * @author Sridhar Paladugu
 * @version 1.0
 *
 */
public class Datasource {
    //load configuration from resources folder
//    private static HikariConfig hikariConfig = new HikariConfig("Database.properties");
    private static HikariDataSource ds;
    static {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl( System.getenv("GP_JDBC_URL") );
        hikariConfig.setUsername( System.getenv("GP_USER"));
        hikariConfig.setPassword( System.getenv("GP_PWD") );
        hikariConfig.addDataSourceProperty( "cachePrepStmts" , "true" );
        hikariConfig.addDataSourceProperty( "prepStmtCacheSize" , "250" );
        hikariConfig.addDataSourceProperty( "prepStmtCacheSqlLimit" , "2048" );
        ds = new HikariDataSource(hikariConfig);
    }
    private Datasource() {}

    public static Connection getConnection() throws SQLException {
        return ds.getConnection();
    }

}
