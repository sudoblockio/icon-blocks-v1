package kafka

import (
	"encoding/binary"
	"io/ioutil"

	"github.com/cenkalti/backoff/v4"
	"github.com/geometry-labs/icon-blocks/config"
	"github.com/riferrei/srclient"
	"go.uber.org/zap"
)

type registerSchemaFunc func(topic string, isKey bool, srcSchemaFile string, forceUpdate bool) (int, error)

// RegisterSchema - register kafka message schema to schemaregistry service
func RegisterSchema(topic string, isKey bool, srcSchemaFile string, forceUpdate bool) (int, error) {
	zap.S().Info("Registering Schema for ", topic)
	schemaRegistryClient := srclient.CreateSchemaRegistryClient("http://" + config.Config.SchemaRegistryURL)
	schema, err := schemaRegistryClient.GetLatestSchema(topic, isKey)
	if schema == nil {
		schema, err = registerSchema(schemaRegistryClient, topic, isKey, srcSchemaFile)
	} else if forceUpdate {
		schema, err = registerSchema(schemaRegistryClient, topic, isKey, srcSchemaFile) //TODO: Resolve update not happening
	}

	if err != nil {
		return 0, err
	}
	schemaIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(schemaIDBytes, uint32(schema.ID()))
	return schema.ID(), nil
}

func registerSchema(schemaRegistryClient *srclient.SchemaRegistryClient, topic string, isKey bool, srcSchemaFile string) (*srclient.Schema, error) {
	//filePath := "/app/schemas/" + srcSchemaFile + ".proto"
	filePath := config.Config.SchemaFolderPath + srcSchemaFile + ".proto"
	zap.S().Info("Adding/Updating Schema from filepath: ", filePath)
	schemaBytes, err := ioutil.ReadFile(filePath)
	if err != nil {
		zap.S().Info("Error reading the schema file, err: ", err)
		return nil, err
	}
	schema, err := schemaRegistryClient.CreateSchema(topic, string(schemaBytes), srclient.Protobuf, isKey)
	if err != nil {
		zap.S().Info("Error creating the schema, err: ", err)
		return nil, err
	}
	return schema, nil
}

// RetriableRegisterSchema - RegisterSchema on retry
func RetriableRegisterSchema(fn registerSchemaFunc, topic string, isKey bool, srcSchemaFile string, forceUpdate bool) (int, error) {
	x := 0
	operation := func() error {
		val, err := fn(topic, isKey, srcSchemaFile, forceUpdate)
		if err != nil {
			zap.S().Info("RegisterSchema unsuccessful")
		} else {
			x = val
		}
		return err
	}
	neb := backoff.NewExponentialBackOff()
	//neb.MaxElapsedTime = time.Minute
	err := backoff.Retry(operation, neb)
	if err != nil {
		zap.S().Info("Finally also RegisterSchema Unsuccessful")
	} else {
		zap.S().Info("Finally RegisterSchema Successful")
	}
	return x, err
}
