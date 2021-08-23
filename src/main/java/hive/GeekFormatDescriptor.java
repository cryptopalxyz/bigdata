package hive;

import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.hive.ql.io.AbstractStorageFormatDescriptor;

import java.util.Set;

public class GeekFormatDescriptor extends AbstractStorageFormatDescriptor {

    @Override
    public Set<String> getNames() {
        return ImmutableSet.of("GeekFile");
    }

    @Override
    public String getInputFormat() {
        return "hive.GeekTextInputFormat";
    }

    @Override
    public String getOutputFormat() {
        return "hive.GeekTextOutputFormat";
    }

}
