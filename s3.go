package s3

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"go.k6.io/k6/metrics"
	"go.k6.io/k6/output"
)

func init() {
	output.RegisterExtension("xk6-output-s3", New)
}

type S3 struct {
	output.SampleBuffer

	params          output.Params
	periodicFlusher *output.PeriodicFlusher

	buffer    *bytes.Buffer
	csvWriter *csv.Writer
	csvLock   sync.Mutex

	tags []string

	client *s3.S3
	cfg    config
}

func New(params output.Params) (output.Output, error) {
	cfg, err := loadConfig()
	if err != nil {
		return nil, err
	}

	buf := bytes.Buffer{}
	systemTags := params.ScriptOptions.SystemTags.Map()

	var tags []string

	for tag, enabled := range systemTags {
		systemTag, err := metrics.SystemTagString(tag)
		if err != nil {
			return nil, err
		}

		// Ignore tags with high cardinality.
		if metrics.NonIndexableSystemTags.Has(systemTag) {
			continue
		}

		if enabled {
			tags = append(tags, tag)
		}
	}

	sort.Strings(tags)

	return &S3{
		params:    params,
		buffer:    &buf,
		csvWriter: csv.NewWriter(&buf),
		tags:      tags,
		cfg:       cfg,
	}, nil
}

// Description returns a short human-readable description of the output.
func (*S3) Description() string {
	return "xk6-output-s3"
}

// Start initializes any state needed for the output, establishes network
// connections, etc.
func (s *S3) Start() error {
	// creating aws session
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(s.cfg.Region),
		Credentials: credentials.NewEnvCredentials(),
	})
	if err != nil {
		return fmt.Errorf("creating aws session: %w", err)
	}

	s.client = s3.New(sess)

	// write csv header
	header := append([]string{"timestamp", "metric_name", "metric_value"}, s.tags...)

	err = s.csvWriter.Write(header)
	if err != nil {
		return fmt.Errorf("writing csv header: %w", err)
	}

	s.csvWriter.Flush()

	pf, err := output.NewPeriodicFlusher(time.Second, s.flushMetrics)
	if err != nil {
		return fmt.Errorf("creating periodic flusher: %w", err)
	}

	s.periodicFlusher = pf

	return nil
}

// flushMetrics Writes samples to the csv file
func (s *S3) flushMetrics() {
	samples := s.GetBufferedSamples()

	if len(samples) > 0 {
		s.csvLock.Lock()
		defer s.csvLock.Unlock()

		for _, sc := range samples {
			for _, sample := range sc.GetSamples() {
				sample := sample

				row := sampleToRow(sample, s.tags)

				err := s.csvWriter.Write(row)
				if err != nil {
					log.Printf("writing to file: %v", err)
				}
			}
		}

		s.csvWriter.Flush()
	}
}

func sampleToRow(sample metrics.Sample, tags []string) []string {
	row := []string{sample.Time.Format(time.RFC3339), sample.Metric.Name, fmt.Sprintf("%f", sample.Value)}
	sampleTags := sample.Tags.Map()

	for _, tag := range tags {
		row = append(row, sampleTags[tag])
	}

	return row
}

// Stop stops the periodic flusher and send the results to AWS S3.
func (s *S3) Stop() error {
	s.periodicFlusher.Stop()

	key := fmt.Sprintf("%d_%s", time.Now().Nanosecond(), s.cfg.Filename)

	if _, err := s.client.PutObject(&s3.PutObjectInput{
		Bucket:      aws.String(s.cfg.Bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(s.buffer.Bytes()),
		ContentType: aws.String("application/csv"),
	}); err != nil {
		return fmt.Errorf("uploading object to aws s3: %w", err)
	}

	return nil
}
