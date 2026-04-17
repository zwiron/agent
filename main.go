package main

import (
	"fmt"
	"os"

	"github.com/kardianos/service"

	// API connectors
	_ "github.com/zwiron/connector/api/airtable"
	_ "github.com/zwiron/connector/api/amplitude"
	_ "github.com/zwiron/connector/api/asana"
	_ "github.com/zwiron/connector/api/bingads"
	_ "github.com/zwiron/connector/api/braze"
	_ "github.com/zwiron/connector/api/dynamics365"
	_ "github.com/zwiron/connector/api/facebookads"
	_ "github.com/zwiron/connector/api/github"
	_ "github.com/zwiron/connector/api/googleads"
	_ "github.com/zwiron/connector/api/googleanalytics"
	_ "github.com/zwiron/connector/api/gsheets"
	_ "github.com/zwiron/connector/api/hubspot"
	_ "github.com/zwiron/connector/api/intercom"
	_ "github.com/zwiron/connector/api/jira"
	_ "github.com/zwiron/connector/api/linkedinads"
	_ "github.com/zwiron/connector/api/mailchimp"
	_ "github.com/zwiron/connector/api/marketo"
	_ "github.com/zwiron/connector/api/mixpanel"
	_ "github.com/zwiron/connector/api/netsuite"
	_ "github.com/zwiron/connector/api/notion"
	_ "github.com/zwiron/connector/api/quickbooks"
	_ "github.com/zwiron/connector/api/rest"
	_ "github.com/zwiron/connector/api/salesforce"
	_ "github.com/zwiron/connector/api/servicenow"
	_ "github.com/zwiron/connector/api/sharepoint"
	_ "github.com/zwiron/connector/api/shopify"
	_ "github.com/zwiron/connector/api/slack"
	_ "github.com/zwiron/connector/api/stripe"
	_ "github.com/zwiron/connector/api/twilio"
	_ "github.com/zwiron/connector/api/webhooks"
	_ "github.com/zwiron/connector/api/workday"
	_ "github.com/zwiron/connector/api/xero"
	_ "github.com/zwiron/connector/api/zendesk"
	_ "github.com/zwiron/connector/api/zuora"

	// Document connectors
	_ "github.com/zwiron/connector/doc/aerospike"
	_ "github.com/zwiron/connector/doc/arangodb"
	_ "github.com/zwiron/connector/doc/cassandra"
	_ "github.com/zwiron/connector/doc/cosmosdb"
	_ "github.com/zwiron/connector/doc/couchbase"
	_ "github.com/zwiron/connector/doc/couchdb"
	_ "github.com/zwiron/connector/doc/documentdb"
	_ "github.com/zwiron/connector/doc/dynamodb"
	_ "github.com/zwiron/connector/doc/elasticsearch"
	_ "github.com/zwiron/connector/doc/firebase"
	_ "github.com/zwiron/connector/doc/firestore"
	_ "github.com/zwiron/connector/doc/memcached"
	_ "github.com/zwiron/connector/doc/mongodb"
	_ "github.com/zwiron/connector/doc/opensearch"
	_ "github.com/zwiron/connector/doc/redis"
	_ "github.com/zwiron/connector/doc/scylladb"
	_ "github.com/zwiron/connector/doc/surrealdb"

	// File connectors
	_ "github.com/zwiron/connector/file/azureblob"
	_ "github.com/zwiron/connector/file/box"
	_ "github.com/zwiron/connector/file/dropbox"
	_ "github.com/zwiron/connector/file/ftp"
	_ "github.com/zwiron/connector/file/gcs"
	_ "github.com/zwiron/connector/file/gdrive"
	_ "github.com/zwiron/connector/file/hdfs"
	_ "github.com/zwiron/connector/file/localfs"
	_ "github.com/zwiron/connector/file/minio"
	_ "github.com/zwiron/connector/file/onedrive"
	_ "github.com/zwiron/connector/file/s3"
	_ "github.com/zwiron/connector/file/sftp"

	// Graph connectors
	_ "github.com/zwiron/connector/graph/dgraph"
	_ "github.com/zwiron/connector/graph/memgraph"
	_ "github.com/zwiron/connector/graph/neo4j"
	_ "github.com/zwiron/connector/graph/neptune"

	// Lake connectors
	_ "github.com/zwiron/connector/lake/delta"
	_ "github.com/zwiron/connector/lake/hudi"
	_ "github.com/zwiron/connector/lake/iceberg"

	// SQL connectors
	_ "github.com/zwiron/connector/sql/cockroachdb"
	_ "github.com/zwiron/connector/sql/firebird"
	_ "github.com/zwiron/connector/sql/mariadb"
	_ "github.com/zwiron/connector/sql/mssql"
	_ "github.com/zwiron/connector/sql/mysql"
	_ "github.com/zwiron/connector/sql/oracle"
	_ "github.com/zwiron/connector/sql/postgres"
	_ "github.com/zwiron/connector/sql/saphana"
	_ "github.com/zwiron/connector/sql/singlestore"
	_ "github.com/zwiron/connector/sql/sqlite"
	_ "github.com/zwiron/connector/sql/tidb"
	_ "github.com/zwiron/connector/sql/vitess"
	_ "github.com/zwiron/connector/sql/yugabytedb"

	// Stream connectors
	_ "github.com/zwiron/connector/stream/eventhubs"
	_ "github.com/zwiron/connector/stream/kafka"
	_ "github.com/zwiron/connector/stream/kinesis"
	_ "github.com/zwiron/connector/stream/mqtt"
	_ "github.com/zwiron/connector/stream/nats"
	_ "github.com/zwiron/connector/stream/pubsub"
	_ "github.com/zwiron/connector/stream/pulsar"
	_ "github.com/zwiron/connector/stream/rabbitmq"
	_ "github.com/zwiron/connector/stream/servicebus"
	_ "github.com/zwiron/connector/stream/sqs"

	// Timeseries connectors
	_ "github.com/zwiron/connector/timeseries/influxdb"
	_ "github.com/zwiron/connector/timeseries/prometheus"
	_ "github.com/zwiron/connector/timeseries/questdb"
	_ "github.com/zwiron/connector/timeseries/timescaledb"
	_ "github.com/zwiron/connector/timeseries/victoriametrics"

	// Vector connectors
	_ "github.com/zwiron/connector/vector/chroma"
	_ "github.com/zwiron/connector/vector/milvus"
	_ "github.com/zwiron/connector/vector/pinecone"
	_ "github.com/zwiron/connector/vector/qdrant"
	_ "github.com/zwiron/connector/vector/weaviate"

	// Warehouse connectors
	_ "github.com/zwiron/connector/warehouse/bigquery"
	_ "github.com/zwiron/connector/warehouse/clickhouse"
	_ "github.com/zwiron/connector/warehouse/databricks"
	_ "github.com/zwiron/connector/warehouse/doris"
	_ "github.com/zwiron/connector/warehouse/exasol"
	_ "github.com/zwiron/connector/warehouse/firebolt"
	_ "github.com/zwiron/connector/warehouse/greenplum"
	_ "github.com/zwiron/connector/warehouse/redshift"
	_ "github.com/zwiron/connector/warehouse/snowflake"
	_ "github.com/zwiron/connector/warehouse/starrocks"
	_ "github.com/zwiron/connector/warehouse/synapse"
	_ "github.com/zwiron/connector/warehouse/vertica"
)

var version = "develop"

func main() {
	// If running as a system service (not interactive), run directly.
	if !service.Interactive() {
		prg := &program{}
		s, err := service.New(prg, svcConfig())
		if err != nil {
			fmt.Fprintf(os.Stderr, "service init: %v\n", err)
			os.Exit(1)
		}
		if err := s.Run(); err != nil {
			fmt.Fprintf(os.Stderr, "service run: %v\n", err)
			os.Exit(1)
		}
		return
	}

	// Interactive mode — parse subcommand.
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	var err error
	switch os.Args[1] {
	case "install":
		err = cmdInstall(os.Args[2:])
	case "uninstall":
		err = cmdUninstall()
	case "start":
		err = cmdStart()
	case "stop":
		err = cmdStop()
	case "restart":
		err = cmdRestart()
	case "status":
		err = cmdStatus()
	case "logs":
		err = cmdLogs()
	case "run":
		// Strip "run" from args so pkg/config doesn't see it.
		os.Args = append(os.Args[:1], os.Args[2:]...)
		err = cmdRun()
	case "update":
		err = cmdUpdate()
	case "reset-certs":
		err = cmdResetCerts()
	case "version":
		fmt.Println(version)
	case "--help", "-h", "help":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}
