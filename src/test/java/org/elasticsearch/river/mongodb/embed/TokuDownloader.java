/**
 * Derived from de.flapdoodle.embed.process.store.Downloader
 */
package org.elasticsearch.river.mongodb.embed;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpCookie;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import de.flapdoodle.embed.process.config.store.IDownloadConfig;
import de.flapdoodle.embed.process.config.store.ITimeoutConfig;
import de.flapdoodle.embed.process.distribution.Distribution;
import de.flapdoodle.embed.process.io.directories.PropertyOrPlatformTempDir;
import de.flapdoodle.embed.process.io.file.Files;
import de.flapdoodle.embed.process.io.progress.IProgressListener;
import de.flapdoodle.embed.process.store.ArtifactStore;
import de.flapdoodle.embed.process.store.IDownloader;

/**
 * Class for downloading runtime
 */
public class TokuDownloader implements IDownloader {

  static final int DEFAULT_CONTENT_LENGTH = 20 * 1024 * 1024;
  static final int BUFFER_LENGTH = 1024 * 8 * 8;
  static final int READ_COUNT_MULTIPLIER = 100;
  private static Logger logger = Logger.getLogger(ArtifactStore.class.getName());

  public TokuDownloader() {

  }

  public String getDownloadUrl(IDownloadConfig runtime, Distribution distribution) {
    return runtime.getDownloadPath().getPath(distribution) + runtime.getPackageResolver().getPath(distribution);
  }

  public File download(IDownloadConfig runtime, Distribution distribution) throws IOException {

    String progressLabel = "Download " + distribution;
    IProgressListener progress = runtime.getProgressListener();
    progress.start(progressLabel);

    ITimeoutConfig timeoutConfig = runtime.getTimeoutConfig();

    URL url = new URL(getDownloadUrl(runtime, distribution));
    HttpURLConnection openConnection = (HttpURLConnection) url.openConnection();

    // Then fetch actual file, using the received cookies
    File ret = Files.createTempFile(PropertyOrPlatformTempDir.defaultInstance(),
            runtime.getFileNaming().nameFor(runtime.getDownloadPrefix(), "." + runtime.getPackageResolver().getArchiveType(distribution)));
    logger.fine("Saving distro to " + ret.getAbsolutePath());
    if (ret.canWrite()) {

      BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(ret));


      openConnection.setRequestProperty("User-Agent",runtime.getUserAgent());
      openConnection.setConnectTimeout(timeoutConfig.getConnectionTimeout());
      openConnection.setReadTimeout(timeoutConfig.getReadTimeout());

        long length = openConnection.getContentLength();
      progress.info(progressLabel, "DownloadSize: " + length);

      if (length == -1) length = DEFAULT_CONTENT_LENGTH;


      long downloadStartedAt = System.currentTimeMillis();

        try (InputStream downloadStream = openConnection.getInputStream()) {
            BufferedInputStream bis = new BufferedInputStream(downloadStream);
            byte[] buf = new byte[BUFFER_LENGTH];
            int read;
            long readCount = 0;
            while ((read = bis.read(buf)) != -1) {
                bos.write(buf, 0, read);
                readCount = readCount + read;
                if (readCount > length) length = readCount;

                progress.progress(progressLabel, (int) (readCount * READ_COUNT_MULTIPLIER / length));
            }
            progress.info(progressLabel, "downloaded with " + downloadSpeed(downloadStartedAt, length));
        } finally {
            bos.flush();
            bos.close();
        }
    } else {
      throw new IOException("Can not write " + ret);
    }
    progress.done(progressLabel);
    return ret;
  }

  private static String downloadSpeed(long downloadStartedAt,long downloadSize) {
    long timeUsed=(System.currentTimeMillis()-downloadStartedAt)/1000;
    if (timeUsed==0) {
      timeUsed=1;
    }
    long kbPerSecond=downloadSize/(timeUsed*1024);
    return ""+kbPerSecond+"kb/s";
  }


}
