package lakeforge.hadoop;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;

/** Local filesystem that does not call Hadoop's Windows native library. */
public class BareRawLocalFileSystem extends RawLocalFileSystem {
    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        File file = pathToFile(path);
        if (!file.exists()) {
            throw new FileNotFoundException(path.toString());
        }
        if (file.isFile()) {
            return new FileStatus[] {getFileStatus(path)};
        }
        File[] children = file.listFiles();
        if (children == null) {
            return new FileStatus[0];
        }
        FileStatus[] statuses = new FileStatus[children.length];
        for (int index = 0; index < children.length; index++) {
            statuses[index] = status(children[index]);
        }
        return statuses;
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        File file = pathToFile(path);
        if (!file.exists()) {
            throw new FileNotFoundException(path.toString());
        }
        return status(file);
    }

    @Override
    public void setPermission(Path path, FsPermission permission) {}

    @Override
    public boolean mkdirs(Path path, FsPermission permission) throws IOException {
        File directory = pathToFile(path);
        return directory.mkdirs() || directory.isDirectory();
    }

    private static FileStatus status(File file) throws IOException {
        return new FileStatus(
                file.length(),
                file.isDirectory(),
                1,
                4096L,
                file.lastModified(),
                file.lastModified(),
                FsPermission.getFileDefault(),
                "lakeforge",
                "lakeforge",
                new Path(file.toURI()));
    }
}
