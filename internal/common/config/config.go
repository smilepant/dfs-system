package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Metadata MetadataConfig `yaml:"metadata"`
	Storage  StorageConfig  `yaml:"storage"`
}

type MetadataConfig struct {
	Port              int      `yaml:"port"`
	DBPath            string   `yaml:"db_path"`
	ReplicationFactor int      `yaml:"replication_factor"`
	EtcdEndpoints     []string `yaml:"etcd_endpoints"`
}

type StorageConfig struct {
	NodeID            string   `yaml:"node_id"`
	Port              int      `yaml:"port"`
	StorageRoot       string   `yaml:"storage_root"`
	EtcdEndpoints     []string `yaml:"etcd_endpoints"`
	HeartbeatInterval int      `yaml:"heartbeat_interval"` // in seconds
	MetadataServerURL string   `yaml:"metadata_server_url"`
}

func LoadConfig(path string) (*Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var cfg Config
	decoder := yaml.NewDecoder(file)
	if err := decoder.Decode(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}
