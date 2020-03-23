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


import com.amazonaws.services.lambda.runtime.Context;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * @author Sridhar Paladugu
 * @version 1.0
 */
public class GreenplumWriter {

    private Datasource datasource;

    public GreenplumWriter() {
    }


    /**
     * get only fileName without extension and replace all hyphens with underscores.
     *
     * @param s3Key
     * @return String
     */
    private String getExternalTableName(String s3Key) {
        String t = s3Key.substring(s3Key.lastIndexOf("/") + 1);
        String f = t.replaceAll("-", "_");
//        String tableName = f.substring(0, f.lastIndexOf("."));
        return f;
//        return tableName;
    }

    private String getDDL(String s3key){
        String tableName = getExternalTableName(s3key);
        StringBuffer sql = new StringBuffer();
        sql.append("create external table ")
            .append("credit_trans.").append(tableName)
            .append(" ( " )
            .append(" account_id integer," )
            .append(" account_lat double precision,")
            .append(" account_long double precision," )
            .append(" account_number text," )
            .append(" card_type text," )
            .append(" fraud_flag boolean," )
            .append(" location_id integer," )
            .append(" merchant_city text," )
            .append(" merchant_city_alias text," )
            .append(" merchant_lat double precision," )
            .append(" merchant_long double precision," )
            .append(" merchant_name text," )
            .append(" merchant_state text," )
            .append(" posting_date text," )
            .append(" rlb_location_key int," )
            .append(" transaction_amount double precision," )
            .append(" transaction_date text," )
            .append(" transaction_id bigint" )
            .append( ") ")
            .append(" location('pxf://") //sppde-greenplumstreams/")
            .append(System.getenv("S3_BUCKET"))
            .append("/")
            .append(s3key)
            .append("?profile=s3:text&SERVER=s3srvcfg')" )
            .append("format 'CSV'");
        return sql.toString();
    }

    private String getDML(String s3key) {
        StringBuffer dml = new StringBuffer();
        String tableName = getExternalTableName(s3key);
        dml.append("INSERT INTO credit_trans.raw_transactions (")
                .append(" account_id, account_lat, account_long, account_number, card_type, ")
                .append(" fraud_flag, location_id, merchant_city, merchant_city_alias, " )
                .append(" merchant_lat, merchant_long, merchant_name, merchant_state, ")
                .append(" posting_date, rlb_location_key, transaction_amount, ")
                .append(" transaction_date, transaction_id) ")
                .append( "SELECT " )
                .append(" account_id, account_lat, account_long, account_number, card_type, ")
                .append(" fraud_flag, location_id, merchant_city, merchant_city_alias, ")
                .append(" merchant_lat, merchant_long, merchant_name, merchant_state, ")
                .append(" posting_date, rlb_location_key, transaction_amount, ")
                .append(" transaction_date, transaction_id")
                .append(" FROM credit_trans.").append(tableName);
        return dml.toString();
    }
    /**
     * Ingest the new file of trnsactions in to Greenplum
     *
     * @param s3Key
     */
    public void writeBatch(String s3Key, Context context) {
        Connection gpConnection = null;
        Statement stmt1 = null;
        Statement stmt2 = null;
        Statement stmt3 = null;
        Boolean rollback = false;
        Exception error = null;
        try {
            gpConnection = Datasource.getConnection();
            gpConnection.setAutoCommit(false);

            String ddl = getDDL(s3Key);
            context.getLogger().log("Creating External Table ...");
            context.getLogger().log(ddl);
            stmt1 = gpConnection.createStatement();
            stmt1.execute(ddl);
            stmt1.close();

            context.getLogger().log("inserting from External Table ...");
            String dml = getDML(s3Key);
            context.getLogger().log(dml);
            stmt2 = gpConnection.createStatement();
            stmt2.execute(dml);
            stmt2.close();

            context.getLogger().log("Deleting external table.");
            stmt3 = gpConnection.createStatement();
            stmt3.execute("DROP EXTERNAL TABLE IF EXISTS credit_trans."+getExternalTableName(s3Key));
            stmt3.close();
            gpConnection.commit();

        } catch (Exception e) {
            context.getLogger().log("Error writing to Greenplum. \n" + e.getMessage());
            rollback = true;
            error = e;
            if (gpConnection != null) {
                try {
                    gpConnection.rollback();
                } catch (SQLException ex) {
                    context.getLogger().log("Error while roll backing  transaction.\n" + ex.getMessage());
                }
            }
        } finally {
            checkAndCloseStatements(stmt1, stmt2, stmt3, context);
            if (gpConnection != null) {
                try {
                    gpConnection.setAutoCommit(true);
                    if (!gpConnection.isClosed()) {
                        gpConnection.close();
                    }
                } catch (SQLException ex) {
                    context.getLogger().log("Error closing connection.");
                    context.getLogger().log(ex.getMessage());
                }
            }
            if(rollback) throw new RuntimeException(error);
        }
    }

    /**
     * Clean up Statement resources
     * @param stmt1
     * @param stmt2
     * @param context
     */
    private void checkAndCloseStatements(Statement stmt1, Statement stmt2, Statement stmt3, Context context) {
        try {
            if (stmt1 != null) {
                if (!stmt1.isClosed())
                    stmt1.close();
            }
        } catch (SQLException ex1) {
            context.getLogger().log(ex1.getMessage());
        }
        try {
            if (stmt2 != null) {
                if (!stmt2.isClosed())
                    stmt2.close();
            }
        } catch (SQLException ex2) {
            context.getLogger().log(ex2.getMessage());
        }
        try {
            if (stmt3 != null) {
                if (!stmt3.isClosed())
                    stmt3.close();
            }
        } catch (SQLException ex3) {
            context.getLogger().log(ex3.getMessage());
        }
    }

}