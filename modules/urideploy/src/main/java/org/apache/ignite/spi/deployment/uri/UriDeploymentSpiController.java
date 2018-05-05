package org.apache.ignite.spi.deployment.uri;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.IgniteSpiMBeanAdapter;
import org.apache.ignite.spi.deployment.uri.scanners.GridUriDeploymentScannerListener;
import org.apache.ignite.spi.deployment.uri.scanners.UriDeploymentScanner;
import org.apache.ignite.spi.deployment.uri.scanners.UriDeploymentScannerManager;
import org.apache.ignite.spi.deployment.uri.scanners.file.UriDeploymentFileScanner;
import org.apache.ignite.spi.deployment.uri.scanners.http.UriDeploymentHttpScanner;

import java.io.File;
import java.io.FilenameFilter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

import static org.apache.ignite.internal.util.IgniteUtils.assertParameter;

public class UriDeploymentSpiController {
    private static final String DFLT_DEPLOY_DIR = "deployment/file";
    private UriDeploymentSpi uriDeploymentSpi;
    @LoggerResource
    private IgniteLogger log;
    private final Collection<UriDeploymentScannerManager> mgrs = new ArrayList<>();
    private Collection<URI> uriEncodedList = new ArrayList<>();
    private List<String> uriList = new ArrayList<>();
    private boolean encodeUri = true;
    private UriDeploymentScanner[] scanners;


    UriDeploymentSpiController(UriDeploymentSpi uriDeploymentSpi) {
        this.uriDeploymentSpi = uriDeploymentSpi;
    }

    public List<String> getUriList() {
        return Collections.unmodifiableList(uriList);
    }

    public void setUriList(List<String> uriList) {
        this.uriList = uriList;
    }

    public void setEncodeUri(boolean encodeUri) {
        this.encodeUri = encodeUri;
    }

    public UriDeploymentScanner[] getScanners() {
        return scanners;
    }

    public void setScanners(UriDeploymentScanner... scanners) {
        this.scanners = scanners;
    }

    public void spiStop() {
        for (UriDeploymentScannerManager mgr : mgrs)
            mgr.cancel();

        for (UriDeploymentScannerManager mgr : mgrs)
            mgr.join();

        // Clear inner collections.
        uriEncodedList.clear();
        mgrs.clear();

        uriDeploymentSpi.releaseAllClassLoaders();
        uriDeploymentSpi.deleteTempDirectory();
    }

    public void spiStart(String igniteInstanceName) throws IgniteSpiException {
        assertParameter(uriList != null, "uriList != null");
        initializeUriList();
        if (uriEncodedList.isEmpty())
            addDefaultUri();
        uriDeploymentSpi.registerBean(igniteInstanceName, new UriDeploymentSpiMBeanImpl(uriDeploymentSpi), UriDeploymentSpiMBean.class);
        configureScanners();
        initializeScannerManagers(igniteInstanceName);
        logSpiStartInfo();
    }

    private void configureScanners() {
        uriDeploymentSpi.firstScanCntr = 0;

        // Set default scanners if none are configured.
        if (scanners == null) {
            scanners = new UriDeploymentScanner[2];

            scanners[0] = new UriDeploymentFileScanner();
            scanners[1] = new UriDeploymentHttpScanner();
        }
    }

    private void initializeScannerManagers(String igniteInstanceName) {
        GridUriDeploymentScannerListener lsnr = uriDeploymentSpi.getNewGridUriDeploymentScannerListener();
        String deployTmpDirPath = uriDeploymentSpi.initializeTemporaryDirectoryPath();
        FilenameFilter filter = (dir, name) -> {
            assert name != null;
            return name.toLowerCase().endsWith(".gar");
        };

        for (URI uri : uriEncodedList) {
            File file = new File(deployTmpDirPath);
            long freq = -1;

            try {
                freq = getFrequencyFromUri(uri);
            }
            catch (NumberFormatException e) {
                U.error(log, "Error parsing parameter value for frequency.", e);
            }

            UriDeploymentScannerManager mgr = null;

            for (UriDeploymentScanner scanner : scanners) {
                if (scanner.acceptsURI(uri)) {
                    mgr = new UriDeploymentScannerManager(igniteInstanceName, uri, file, freq > 0 ? freq :
                            scanner.getDefaultScanFrequency(), filter, lsnr, log, scanner);
                    break;
                }
            }

            if (mgr == null)
                throw new IgniteSpiException("Unsupported URI (please configure appropriate scanner): " + uri);

            mgrs.add(mgr);
            mgr.start();
        }
    }

    private void logSpiStopInfo() {
        // Ack ok stop.
        if (log.isDebugEnabled())
            log.debug(uriDeploymentSpi.getStopInfo());
    }

    private void logSpiStartInfo() {
        // Ack parameters.
        if (log.isDebugEnabled()) {
            log.debug(uriDeploymentSpi.getConfigInfo("uriList", uriList));
            log.debug(uriDeploymentSpi.getConfigInfo("encodeUri", encodeUri));
            log.debug(uriDeploymentSpi.getConfigInfo("scanners", mgrs));
        }

        // Ack ok start.
        if (log.isDebugEnabled())
            log.debug(uriDeploymentSpi.getStartInfo());
    }

    private void initializeUriList() throws IgniteSpiException {
        for (String uri : uriList) {
            assertParameter(uri != null, "uriList.get(X) != null");
            String encUri = encodeUri(uri.replaceAll("\\\\", "/"));
            URI uriObj;

            try {
                uriObj = new URI(encUri);
            }
            catch (URISyntaxException e) {
                throw new IgniteSpiException("Failed to parse URI [uri=" + U.hidePassword(uri) +
                        ", encodedUri=" + U.hidePassword(encUri) + ']', e);
            }

            if (uriObj.getScheme() == null || uriObj.getScheme().trim().isEmpty())
                throw new IgniteSpiException("Failed to get 'scheme' from URI [uri=" +
                        U.hidePassword(uri) +
                        ", encodedUri=" + U.hidePassword(encUri) + ']');

            uriEncodedList.add(uriObj);
        }
    }

    private void addDefaultUri() throws IgniteSpiException {
        assert uriEncodedList != null;

        URI uri;

        try {
            uri = U.resolveWorkDirectory(uriDeploymentSpi.getConfigDirectory(), DFLT_DEPLOY_DIR, false).toURI();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteSpiException("Failed to initialize default file scanner", e);
        }

        uriEncodedList.add(uri);
    }

    private long getFrequencyFromUri(URI uri) throws NumberFormatException {
        assert uri != null;

        String userInfo = uri.getUserInfo();

        if (userInfo != null) {
            String[] arr = userInfo.split(";");

            if (arr.length > 0)
                for (String el : arr)
                    if (el.startsWith("freq="))
                        return Long.parseLong(el.substring(5));
        }

        return -1;
    }

    private String encodeUri(String path) {
        return encodeUri ? new GridUriDeploymentUriParser(path).parse() : path;
    }

    public boolean isFirstScanFinished(int cntr) {
        assert uriEncodedList != null;

        return cntr >= uriEncodedList.size();
    }

    /**
     * MBean implementation for UriDeploymentSpi.
     */
    protected class UriDeploymentSpiMBeanImpl extends IgniteSpiMBeanAdapter implements UriDeploymentSpiMBean {
        /** {@inheritDoc} */
        UriDeploymentSpiMBeanImpl(IgniteSpiAdapter spiAdapter) {
            super(spiAdapter);
        }

        /** {@inheritDoc} */
        @Override public String getTemporaryDirectoryPath() {
            return uriDeploymentSpi.getTemporaryDirectoryPath();
        }

        /** {@inheritDoc} */
        @Override public List<String> getUriList() {
            return  uriDeploymentSpi.getUriList();
        }

        /** {@inheritDoc} */
        @Override public boolean isCheckMd5() {
            return  uriDeploymentSpi.isCheckMd5();
        }
    }
}
