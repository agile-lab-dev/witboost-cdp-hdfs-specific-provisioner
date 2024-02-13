package it.agilelab.witboost.cdp.priv.hdfs.provisioning.service;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;

import io.vavr.control.Either;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.common.FailedOperation;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.common.Problem;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.config.HdfsConfig;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.model.HdfsResult;
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
import org.springframework.web.client.RestClientResponseException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.DefaultUriBuilderFactory;
import org.springframework.web.util.UriComponentsBuilder;

@Service
public class HdfsServiceImpl implements HdfsService {

    private final Logger logger = LoggerFactory.getLogger(HdfsServiceImpl.class);

    private final RestTemplate restTemplate;

    public HdfsServiceImpl(RestTemplate restTemplate, HdfsConfig hdfsConfig) {
        this.restTemplate = restTemplate;
        this.restTemplate.setUriTemplateHandler(new DefaultUriBuilderFactory(hdfsConfig.baseUrl()));
        if (this.restTemplate.getRequestFactory() instanceof HttpComponentsClientHttpRequestFactory f) {
            f.setConnectTimeout(hdfsConfig.timeout());
            f.setConnectionRequestTimeout(hdfsConfig.timeout());
        }
    }

    @Override
    public Either<FailedOperation, String> createFolder(String path) {
        try {
            ResponseEntity<HdfsResult> response =
                    restTemplate.exchange(
                            buildCreateUrl(path), HttpMethod.PUT, HttpEntity.EMPTY, HdfsResult.class);
            HdfsResult hdfsResult = response.getBody();
            if (hdfsResult != null && hdfsResult.isOutcome()) {
                return right(path);
            }
            return left(
                    new FailedOperation(
                            Collections.singletonList(
                                    new Problem(getFailedMessage("create", path, Optional.empty())))));
        } catch (RestClientResponseException rcre) {
            logger.error("Error in createFolder", rcre);
            return left(
                    new FailedOperation(
                            Collections.singletonList(
                                    new Problem(getFailedMessage("create", path, Optional.of(rcre)), rcre))));
        }
    }

    @Override
    public Either<FailedOperation, String> deleteFolder(String path) {
        try {
            ResponseEntity<HdfsResult> response =
                    restTemplate.exchange(
                            buildDeleteUrl(path), HttpMethod.DELETE, HttpEntity.EMPTY, HdfsResult.class);
            HdfsResult hdfsResult = response.getBody();
            // if the folder doesn't exist (maybe is already deleted), outcome is false, so it's ok for us
            if (hdfsResult != null) {
                return right(path);
            }
            return left(
                    new FailedOperation(
                            Collections.singletonList(
                                    new Problem(getFailedMessage("delete", path, Optional.empty())))));
        } catch (RestClientResponseException rcre) {
            logger.error("Error in deleteFolder", rcre);
            return left(
                    new FailedOperation(
                            Collections.singletonList(
                                    new Problem(getFailedMessage("delete", path, Optional.of(rcre)), rcre))));
        }
    }

    private String buildCreateUrl(String path) {
        String url = "/webhdfs/v1{path}";
        Map<String, String> urlParams = new HashMap<>();
        urlParams.put("path", path);
        UriComponentsBuilder builder =
                UriComponentsBuilder.fromUriString(url).queryParam("op", "MKDIRS");
        return builder.buildAndExpand(urlParams).toString();
    }

    private String buildDeleteUrl(String path) {
        String url = "/webhdfs/v1{path}";
        Map<String, String> urlParams = new HashMap<>();
        urlParams.put("path", path);
        UriComponentsBuilder builder =
                UriComponentsBuilder.fromUriString(url)
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
}
