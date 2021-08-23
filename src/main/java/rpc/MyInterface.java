package rpc;

import org.apache.hadoop.ipc.VersionedProtocol;

public interface MyInterface extends VersionedProtocol {
    long versionID = 1L;
    int add(int number1, int number2);
    String showName(long id);
}