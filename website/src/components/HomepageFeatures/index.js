import React from "react";
import PropTypes from "prop-types";
import clsx from "clsx";
import styles from "./styles.module.css";
import Heading from "@theme/Heading";

const FeatureList = [
    {
        title: "Open source",
        description: (
            <p>
                A free and Open Source drop-in replacement for Confluent Schema
                Registry and the Kafka REST Proxy. Use it with any managed
                provider or host it yourself, and join the direction of the
                project.
            </p>
        ),
    },
    {
        title: "Schema hub",
        description: (
            <p>
                A schema registry for your data schema versions, with support
                for Avro, JSON Schema and Protobuf formats and full
                compatibility checking.
            </p>
        ),
    },
    {
        title: "REST API",
        description: (
            <p>
                A powerful HTTP interface to your Apache Kafka® clusters for
                producing and consuming messages without a native Kafka client.
            </p>
        ),
    },
    {
        title: "Broad adoption",
        description: (
            <p>
                Trusted, operated at scale and actively developed by some of the
                biggest Apache Kafka service providers around. Battle-tested and
                serving many organisations well.
            </p>
        ),
    },
    {
        title: "Reliable",
        description: (
            <p>
                A leader/replica architecture ensures high availability and sane
                load-balancing across your Schema Registry instances.
            </p>
        ),
    },
    {
        title: "Observable",
        description: (
            <p>
                Built-in metrics and OpenTelemetry support give operations teams
                the visibility they need to run Karapace in production.
            </p>
        ),
    },
];

function Feature({ title, description }) {
    return (
        <div className={clsx("col col--4", styles.featureCol)}>
            <div className={clsx("card", styles.featureCard)}>
                <div className="card__header">
                    <Heading as={"h3"}>{title}</Heading>
                </div>
                <div className="card__body">{description}</div>
            </div>
        </div>
    );
}

Feature.propTypes = {
    title: PropTypes.string,
    description: PropTypes.node,
};

export default function HomepageFeatures() {
    return (
        <section className={styles.features}>
            <div className="container">
                <div className="text--center">
                    <Heading as={"h2"} id={"key-features"}>
                        Key features
                    </Heading>
                </div>
                <div className="row">
                    {FeatureList.map((props, idx) => (
                        <Feature key={idx} {...props} />
                    ))}
                </div>
            </div>
        </section>
    );
}
