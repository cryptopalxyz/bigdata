package rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

public class RPCClient {
    public static void main(String[] args) throws Exception {
        try {
            MyInterface proxy = RPC.getProxy(MyInterface.class, 1L,
                    new InetSocketAddress("127.0.0.1",12345), new Configuration());
            String res = proxy.showName(20210579030237L);
            System.out.println(res);
            res = proxy.showName(202105790302399L);
            System.out.println(res);
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

}
