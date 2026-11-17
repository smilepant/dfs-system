package config

import (
	"os"
	"strings"

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
	RepairInterval    int      `yaml:"repair_interval"` // in seconds; 0 = default, <0 = disabled
}

type StorageConfig struct {
	NodeID            string   `yaml:"node_id"`
	Port              int      `yaml:"port"`
	StorageRoot       string   `yaml:"storage_root"`
	EtcdEndpoints     []string `yaml:"etcd_endpoints"`
	HeartbeatInterval int      `yaml:"heartbeat_interval"` // in seconds
	MetadataServerURL string   `yaml:"metadata_server_url"`
	AdvertiseAddress  string   `yaml:"advertise_address"` // host:port other services use to reach this node
	CapacityBytes     int64    `yaml:"capacity_bytes"`
}

// LoadConfig reads metadata and storage settings from a YAML file.
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

// SplitList splits a comma-separated flag value, trimming blanks.
func SplitList(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}
