package org.apache.ignite.spi.deployment.uri;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiConfiguration;
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

public class UriDeploymentSpiController extends IgniteSpiAdapter {
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

    public UUID getNodeId() {
        return ignite.configuration().getNodeId();
    }

    /**
     * Gets list of URIs that are processed by SPI.
     *
     * @return List of URIs.
     */
    public List<String> getUriList() {
        return Collections.unmodifiableList(uriList);
    }

    /**
     * Sets list of URI which point to GAR file and which should be
     * scanned by SPI for the new tasks.
     * <p>
     * If not provided, default value is list with
     * {@code file://${IGNITE_HOME}/work/deployment/file} element.
     * Note that system property {@code IGNITE_HOME} must be set.
     * For unknown {@code IGNITE_HOME} list of URI must be provided explicitly.
     *
     * @param uriList GAR file URIs.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public UriDeploymentSpiController setUriList(List<String> uriList) {
        this.uriList = uriList;

        return this;
    }

    public void setEncodeUri(boolean encodeUri) {
        this.encodeUri = encodeUri;
    }

    /**
     * Gets scanners.
     *
     * @return Scanners.
     */
    public UriDeploymentScanner[] getScanners() {
        return scanners;
    }

    /**
     * Sets scanners.
     *
     * @param scanners Scanners.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public UriDeploymentSpiController setScanners(UriDeploymentScanner... scanners) {
        this.scanners = scanners;

        return this;
    }

    /** {@inheritDoc} */
    public IgniteSpiAdapter setName(String name) {
        super.setName(name);

        return this;
    }

    @Override public void spiStop() {
        for (UriDeploymentScannerManager mgr : mgrs)
            mgr.cancel();

        for (UriDeploymentScannerManager mgr : mgrs)
            mgr.join();

        // Clear inner collections.
        uriEncodedList.clear();
        mgrs.clear();

        uriDeploymentSpi.releaseAllClassLoaders();
        unregisterMBean();
        uriDeploymentSpi.deleteTempDirectory();
        logSpiStopInfo();
    }

    @Override public void spiStart(String igniteInstanceName) throws IgniteSpiException {
        startStopwatch();
        assertParameter(uriList != null, "uriList != null");
        initializeUriList();
        if (uriEncodedList.isEmpty())
            addDefaultUri();
        registerMBean(igniteInstanceName, new UriDeploymentSpiMBeanImpl(this), UriDeploymentSpiMBean.class);
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
            log.debug(stopInfo());
    }

    private void logSpiStartInfo() {
        // Ack parameters.
        if (log.isDebugEnabled()) {
            log.debug(configInfo("uriList", uriList));
            log.debug(configInfo("encodeUri", encodeUri));
            log.debug(configInfo("scanners", mgrs));
        }

        // Ack ok start.
        if (log.isDebugEnabled())
            log.debug(startInfo());
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
            uri = U.resolveWorkDirectory(ignite.configuration().getWorkDirectory(), DFLT_DEPLOY_DIR, false).toURI();
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

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(UriDeploymentSpiController.class, this);
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
            return  getUriList();
        }

        /** {@inheritDoc} */
        @Override public boolean isCheckMd5() {
            return  uriDeploymentSpi.isCheckMd5();
        }
    }
}
