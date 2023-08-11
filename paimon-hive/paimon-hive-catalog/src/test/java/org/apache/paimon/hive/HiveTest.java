package org.apache.paimon.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.hadoop.HadoopFileIO;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.UUID;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTORECONNECTURLKEY;

public class HiveTest {
    public static void main(String[] args) {
        // 进行Kerberos 认证
        System.setProperty("HADOOP_USER_NAME", "hive");
        System.setProperty("java.security.krb5.realm", "UAT.JZBD.COM");
        System.setProperty("java.security.krb5.kdc", "tmaster:88");

        Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", "Kerberos");
        UserGroupInformation.setConfiguration(conf);
        try {
            UserGroupInformation.loginUserFromKeytab("meixb001/user@UAT.JZBD.COM", "/home/chouc/meixb001.user.keytab");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        HiveConf hiveConf = new HiveConf();
        HiveMetaStoreClient metaStoreClient = null;
        try {
            metaStoreClient = new HiveMetaStoreClient(hiveConf);
        } catch (MetaException e) {
            throw new RuntimeException(e);
        }
        String metastoreClientClass = "org.apache.hadoop.hive.metastore.HiveMetaStoreClient";
        FileIO fileIO = new HadoopFileIO();
        HiveCatalog catalog = new HiveCatalog(fileIO, hiveConf, metastoreClientClass, "/user/hive/paimon");
        System.out.println(catalog.listDatabases());
        System.out.println(catalog.listDatabases());
    }
}
