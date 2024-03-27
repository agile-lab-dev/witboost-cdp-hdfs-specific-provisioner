package it.agilelab.witboost.cdp.priv.hdfs.provisioning.service;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;

import io.vavr.control.Either;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.common.FailedOperation;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.common.Problem;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.config.HdfsConfig;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.model.HdfsResult;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.model.JmxResponse;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestClientResponseException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

@Service
public class HdfsServiceImpl implements HdfsService {

    private final Logger logger = LoggerFactory.getLogger(HdfsServiceImpl.class);

    private final RestTemplate restTemplate;

    private final HdfsConfig hdfsConfig;

    public HdfsServiceImpl(RestTemplate restTemplate, HdfsConfig hdfsConfig) {
        this.restTemplate = restTemplate;
        this.hdfsConfig = hdfsConfig;
        if (this.restTemplate.getRequestFactory() instanceof HttpComponentsClientHttpRequestFactory f) {
            f.setConnectTimeout(hdfsConfig.timeout());
            f.setConnectionRequestTimeout(hdfsConfig.timeout());
        }
    }

    @Override
    public Either<FailedOperation, String> createFolder(String path) {
        try {
            var activeNNBaseUrl = getActiveNNBaseUrl();
            if (activeNNBaseUrl.isLeft()) return left(activeNNBaseUrl.getLeft());
            ResponseEntity<HdfsResult> response = restTemplate.exchange(
                    buildCreateUrl(activeNNBaseUrl.get(), path), HttpMethod.PUT, HttpEntity.EMPTY, HdfsResult.class);
            HdfsResult hdfsResult = response.getBody();
            if (hdfsResult != null && hdfsResult.isOutcome()) {
                return right(path);
            }
            return left(new FailedOperation(
                    Collections.singletonList(new Problem(getFailedMessage("create", path, Optional.empty())))));
        } catch (RestClientResponseException rcre) {
            logger.error("Error in createFolder", rcre);
            return left(new FailedOperation(
                    Collections.singletonList(new Problem(getFailedMessage("create", path, Optional.of(rcre)), rcre))));
        }
    }

    @Override
    public Either<FailedOperation, String> deleteFolder(String path) {
        try {
            var activeNNBaseUrl = getActiveNNBaseUrl();
            if (activeNNBaseUrl.isLeft()) return left(activeNNBaseUrl.getLeft());
            ResponseEntity<HdfsResult> response = restTemplate.exchange(
                    buildDeleteUrl(activeNNBaseUrl.get(), path), HttpMethod.DELETE, HttpEntity.EMPTY, HdfsResult.class);
            HdfsResult hdfsResult = response.getBody();
            // if the folder doesn't exist (maybe is already deleted), outcome is false, so it's ok for us
            if (hdfsResult != null) {
                return right(path);
            }
            return left(new FailedOperation(
                    Collections.singletonList(new Problem(getFailedMessage("delete", path, Optional.empty())))));
        } catch (RestClientResponseException rcre) {
            logger.error("Error in deleteFolder", rcre);
            return left(new FailedOperation(
                    Collections.singletonList(new Problem(getFailedMessage("delete", path, Optional.of(rcre)), rcre))));
        }
    }

    private String buildCreateUrl(String baseUrl, String path) {
        String url = "/webhdfs/v1{path}";
        Map<String, String> urlParams = new HashMap<>();
        urlParams.put("path", path);
        UriComponentsBuilder builder =
                UriComponentsBuilder.fromUriString(baseUrl).path(url).queryParam("op", "MKDIRS");
        return builder.buildAndExpand(urlParams).toString();
    }

    private String buildDeleteUrl(String baseUrl, String path) {
        String url = "/webhdfs/v1{path}";
        Map<String, String> urlParams = new HashMap<>();
        urlParams.put("path", path);
        UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(baseUrl)
                .path(url)
                .queryParam("op", "DELETE")
                .queryParam("recursive", "true");
        return builder.buildAndExpand(urlParams).toString();
    }

    private String getFailedMessage(String operation, String path, Optional<Throwable> ex) {
        if (ex.isPresent()) {
            return String.format(
                    "Failed to %s the folder '%s'. Please try again and if the issue persists contact the platform team. Details: %s",
                    operation, path, ex.get().getMessage());
        }
        return String.format(
                "Failed to %s the folder '%s'. Please try again and if the issue persists contact the platform team",
                operation, path);
    }

    private Either<FailedOperation, String> getActiveNNBaseUrl() {
        try {
            String jmxUrl = "/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus";

            logger.info("Checking if NN1 is active");
            String fullUrlNN1 = UriComponentsBuilder.fromUriString(hdfsConfig.baseUrlNN1())
                    .path(jmxUrl)
                    .build()
                    .toString();
            JmxResponse resNN1 = restTemplate.getForObject(fullUrlNN1, JmxResponse.class);
            if (resNN1 != null
                    && resNN1.getBeans() != null
                    && resNN1.getBeans().stream().anyMatch(b -> "active".equalsIgnoreCase(b.getState())))
                return right(hdfsConfig.baseUrlNN1());

            logger.info("Checking if NN2 is active");
            String fullUrlNN2 = UriComponentsBuilder.fromUriString(hdfsConfig.baseUrlNN2())
                    .path(jmxUrl)
                    .build()
                    .toString();
            JmxResponse resNN2 = restTemplate.getForObject(fullUrlNN2, JmxResponse.class);
            if (resNN2 != null
                    && resNN2.getBeans() != null
                    && resNN2.getBeans().stream().anyMatch(b -> "active".equalsIgnoreCase(b.getState())))
                return right(hdfsConfig.baseUrlNN2());

            String errorMessage =
                    "Unable to find an active NameNode. Please try again and if the issue persists contact the platform team.";
            logger.error(errorMessage);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage))));

        } catch (RestClientException rce) {
            logger.error("Error in getActiveNN", rce);
            return left(new FailedOperation(Collections.singletonList(new Problem(
                    "Failed to retrieve the current active NameNode. Please try again and if the issue persists contact the platform team. Details: "
                            + rce.getMessage(),
                    rce))));
        }
    }
}
