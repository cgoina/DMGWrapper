package config

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"log"
)

// Config application settings
type Config map[string]interface{}

// GetConfig initialize a config from the given filenames.
func GetConfig(cfgFileNames ...string) (*Config, error) {
	cfg := &Config{}
	for _, cfgFileName := range cfgFileNames {
		if err := cfg.readConfig(cfgFileName); err != nil {
			return cfg, err
		}
	}
	return cfg, nil
}

func (cfg *Config) readConfig(cfgFileName string) error {
	cfgContent, err := ioutil.ReadFile(cfgFileName)
	if err != nil {
		log.Printf("Error reading config file %s: %v", cfgFileName, err)
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(cfgContent))
	var config map[string]interface{}
	err = decoder.Decode(&config)
	if err != nil {
		log.Printf("Error reading JSON from config file %s: %v", cfgFileName, err)
		return err
	}
	for k, v := range config {
		(*cfg)[k] = v
	}
	return nil
}

// GetIntProperty get the property value as an int; if the property does not exist
// or if it's not a number it returns 0
func (cfg Config) GetIntProperty(name string) (res int) {
	if cfg[name] != nil {
		switch v := cfg[name].(type) {
		case int:
			return v
		case int32:
			return int(v)
		case int64:
			return int(v)
		case float64:
			return int(v)
		default:
			log.Printf("Expected int64 value for %s: %v", name, v)
		}
	}
	return 0
}

// GetInt64Property get the property value as an int64; if the property does not exist
// or if it's not a number it returns 0
func (cfg Config) GetInt64Property(name string) (res int64) {
	if cfg[name] != nil {
		switch v := cfg[name].(type) {
		case int:
			return int64(v)
		case int32:
			return int64(v)
		case int64:
			return v
		case float64:
			return int64(v)
		default:
			log.Printf("Expected int64 value for %s: %v", name, v)
		}
	}
	return 0
}

// GetStringProperty - read a string property
func (cfg Config) GetStringProperty(name string) (res string) {
	defer func() {
		if r := recover(); r != nil {
			res = ""
		}
	}()
	if cfg[name] != nil {
		return cfg[name].(string)
	}
	return ""
}

// GetStringArrayProperty - read a string array property
func (cfg Config) GetStringArrayProperty(name string) (res []string) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Error encountered while reading string array property: %s - %v", name, r)
		}
	}()
	switch v := cfg[name].(type) {
	case []string:
		res = v
	case string:
		res = []string{v}
	case []interface{}:
		res = make([]string, len(v))
		for i, vi := range v {
			res[i] = vi.(string)
		}
	case interface{}:
		res = []string{v.(string)}
	default:
		res = []string{}
	}
	return res
}

// GetStringMapProperty - read a map from string to string property
func (cfg Config) GetStringMapProperty(name string) (res map[string]string) {
	res = map[string]string{}
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Error encountered while reading string map property: %s - %v", name, r)
		}
	}()
	if cfg[name] != nil {
		for k, v := range cfg[name].(map[string]interface{}) {
			res[k] = v.(string)
		}
	}
	return res
}
