package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type config struct {
	data     map[string]string
	state    bool
	env_path string
}

func NewLoggerConfig(path string) *config {
	return &config{
		state:    false,
		env_path: path,
	}
}

func (cfg *config) LoadConfig() error {

	if cfg.state {
		return nil
	}

	yamlFile, err := os.ReadFile(cfg.env_path)
	if err != nil {
		return fmt.Errorf("Error reading config file: %w", err)
	}

	err = yaml.Unmarshal(yamlFile, &cfg.data)
	if err != nil {
		return fmt.Errorf("Unable to marshall the config file: %v", err)
	}

	cfg.state = true
	return nil
}

func (cfg *config) GetConfig(key string, defaultValue string) (string, error) {
	value, ok := cfg.data[key]
	if !ok {
		return defaultValue, fmt.Errorf("Key (%s) not found in config file. Falling back to default value ('%s')", key, defaultValue)
	}
	return value, nil
}
