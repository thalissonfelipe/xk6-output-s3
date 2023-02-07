package s3

import (
	"fmt"

	"github.com/ardanlabs/conf/v3"
)

type config struct {
	Region          string `conf:"env:AWS_REGION,required:true"`
	AccessKey       string `conf:"env:AWS_ACCESS_KEY_ID,required:true"`
	SecretAccessKey string `conf:"env:AWS_SECRET_ACCESS_KEY,required:true"`
	Bucket          string `conf:"env:AWS_BUCKET,required:true"`
	Filename        string `conf:"env:AWS_FILENAME,required:true"`
}

func loadConfig() (config, error) {
	const noPrefix = ""

	var cfg config
	if _, err := conf.Parse(noPrefix, &cfg); err != nil {
		return cfg, fmt.Errorf("parsing config: %w", err)
	}

	return cfg, nil
}
