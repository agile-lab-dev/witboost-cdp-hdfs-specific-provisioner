package it.agilelab.witboost.cdp.priv.hdfs.provisioning.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "ranger")
public record RangerConfig(String baseUrl, Integer timeout) {}
