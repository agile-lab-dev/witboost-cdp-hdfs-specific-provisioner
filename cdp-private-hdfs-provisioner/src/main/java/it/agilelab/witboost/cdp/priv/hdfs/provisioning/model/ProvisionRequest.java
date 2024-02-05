package it.agilelab.witboost.cdp.priv.hdfs.provisioning.model;

public record ProvisionRequest<T>(
        DataProduct dataProduct, Component<T> component, Boolean removeData) {}
