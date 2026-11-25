package lakeforge.hadoop;

import org.apache.hadoop.fs.LocalFileSystem;

/** Checksum filesystem backed by {@link BareRawLocalFileSystem}. */
public class BareLocalFileSystem extends LocalFileSystem {
    public BareLocalFileSystem() {
        super(new BareRawLocalFileSystem());
    }
}
