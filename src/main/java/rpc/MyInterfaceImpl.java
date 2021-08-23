package rpc;

import org.apache.hadoop.ipc.ProtocolSignature;

import java.io.IOException;

public class MyInterfaceImpl implements MyInterface{
    public int add(int number1, int number2) {
        System.out.println("number1 = " + number1 + " number2 = " + number2);
        return number1 + number2;
    }

    public String showName(long id) {
        if (id == 20210579030237L)
            return "猩猩";
        return "学号没找到";
    }

    public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
        return MyInterface.versionID;
    }

    public ProtocolSignature getProtocolSignature(String s, long l, int i) throws IOException {
        return null;
    }
}
