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

import com.amazonaws.services.kms.AWSKMS;
import com.amazonaws.services.kms.AWSKMSClientBuilder;
import com.amazonaws.services.kms.model.DecryptRequest;
import com.amazonaws.util.Base64;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
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
        hikariConfig.setJdbcUrl( decryptKey("GP_JDBC_URL") );
        hikariConfig.setUsername( decryptKey("GP_USER"));
        hikariConfig.setPassword( decryptKey("GP_PWD") );
        hikariConfig.addDataSourceProperty( "cachePrepStmts" , "true" );
        hikariConfig.addDataSourceProperty( "prepStmtCacheSize" , "250" );
        hikariConfig.addDataSourceProperty( "prepStmtCacheSqlLimit" , "2048" );
        ds = new HikariDataSource(hikariConfig);
    }
    /**
     * Decrypt the lambda console key
     *
     * @param keyName
     * @return
     */
    private static String decryptKey(String keyName) {
        System.out.println("Decrypting key");
        byte[] encryptedKey = Base64.decode(System.getenv(keyName));

        AWSKMS client = AWSKMSClientBuilder.defaultClient();

        DecryptRequest request = new DecryptRequest()
                .withCiphertextBlob(ByteBuffer.wrap(encryptedKey));

        ByteBuffer plainTextKey = client.decrypt(request).getPlaintext();
        return new String(plainTextKey.array(), Charset.forName("UTF-8"));
    }

    private Datasource() {}

    public static Connection getConnection() throws SQLException {
        return ds.getConnection();
    }

}
