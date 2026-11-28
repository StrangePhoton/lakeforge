package lakeforge.hadoop;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegateToFileSystem;

/** FileContext view of the local disk that does not call the Windows native library. */
public class BareLocalFs extends DelegateToFileSystem {
    public BareLocalFs(URI theUri, Configuration conf) throws IOException, URISyntaxException {
        super(theUri, new BareRawLocalFileSystem(), conf, "file", false);
    }
}
