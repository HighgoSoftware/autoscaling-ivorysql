package billing

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/lithammer/shortuuid"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var hostname string

func init() {
	var err error
	hostname, err = os.Hostname()
	if err != nil {
		hostname = fmt.Sprintf("unknown-%d", rand.Intn(1000))
	}
}

// GetHostname returns the hostname to be used for enriching billing events (see Enrich())
//
// This function MUST NOT be run before init has finished.
func GetHostname() string {
	return hostname
}

type Client interface {
	Send(ctx context.Context, payload []byte, traceID TraceID) error
	LogFields() zap.Field
}

type TraceID string

func GenerateTraceID() TraceID {
	return TraceID(shortuuid.New())
}

type HTTPClient struct {
	URL   string
	httpc *http.Client
}

func NewHTTPClient(url string, c *http.Client) HTTPClient {
	return HTTPClient{URL: fmt.Sprintf("%s/usage_events", url), httpc: c}
}

func (c HTTPClient) Send(ctx context.Context, payload []byte, traceID TraceID) error {
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, c.URL, bytes.NewReader(payload))
	if err != nil {
		return RequestError{Err: err}
	}
	r.Header.Set("content-type", "application/json")
	r.Header.Set("x-trace-id", string(traceID))

	resp, err := c.httpc.Do(r)
	if err != nil {
		return RequestError{Err: err}
	}
	defer resp.Body.Close()

	// theoretically if wanted/needed, we should use an http handler that
	// does the retrying, to avoid writing that logic here.
	if resp.StatusCode != http.StatusOK {
		return UnexpectedStatusCodeError{StatusCode: resp.StatusCode}
	}

	return nil
}

func (c HTTPClient) LogFields() zap.Field {
	return zap.String("url", c.URL)
}

type S3ClientConfig struct {
	Bucket         string `json:"bucket"`
	Region         string `json:"region"`
	PrefixInBucket string `json:"prefixInBucket"`
	Endpoint       string `json:"endpoint"`
}

type S3Client struct {
	cfg    S3ClientConfig
	client *s3.Client
	now    func() time.Time
}

func NewS3Client(cfg S3ClientConfig, now func() time.Time) (S3Client, error) {
	s3Config, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(cfg.Region))

	if err != nil {
		return S3Client{}, err
	}

	client := s3.NewFromConfig(s3Config, func(o *s3.Options) {
		o.BaseEndpoint = &cfg.Endpoint
		o.UsePathStyle = true
	})

	return S3Client{
		cfg:    cfg,
		client: client,
		now:    now,
	}, nil
}

func (c S3Client) key() string {
	// Example: year=2021/month=01/day=26/hh:mm:ssZ_{autoscaler_agent_id}.ndjson.gz
	now := c.now()
	id := shortuuid.New()

	filename := fmt.Sprintf("year=%d/month=%02d/day=%02d/%s_%s.ndjson.gz",
		now.Year(), now.Month(), now.Day(),
		now.Format("15:04:05Z"),
		id,
	)
	return fmt.Sprintf("%s/%s", c.cfg.PrefixInBucket, filename)
}

type s3LogFields struct {
	S3Client
}

func (c s3LogFields) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("bucket", c.cfg.Bucket)
	enc.AddString("prefixInBucket", c.cfg.PrefixInBucket)
	return nil
}

func (c S3Client) LogFields() zap.Field {
	return zap.Inline(s3LogFields{c})
}

func (c S3Client) Send(ctx context.Context, payload []byte, traceID TraceID) error {
	key := c.key()
	buf := bytes.Buffer{}

	gzW := gzip.NewWriter(&buf)
	_, err := gzW.Write(payload)
	_ = gzW.Close()

	if err != nil {
		return RequestError{Err: err}
	}

	r := bytes.NewReader(buf.Bytes())
	_, err = c.client.PutObject(ctx, &s3.PutObjectInput{ //nolint:exhaustruct // AWS SDK
		Bucket: &c.cfg.Bucket,
		Key:    &key,
		Body:   r,
	})

	if err != nil {
		return RequestError{Err: err}
	}

	return nil
}

// Enrich sets the event's Type and IdempotencyKey fields, so that users of this API don't need to
// manually set them
func Enrich[E Event](now time.Time, hostname string, countInBatch, batchSize int, event E) E {
	event.setType()

	// RFC3339 with microsecond precision. Possible to get collisions with millis, nanos are extra.
	// And everything's in UTC, so there's no sense including the offset.
	formattedTime := now.In(time.UTC).Format("2006-01-02T15:04:05.999999Z")

	key := event.getIdempotencyKey()
	if *key == "" {
		*key = fmt.Sprintf("%s-%s-%d/%d", formattedTime, hostname, countInBatch, batchSize)
	}

	return event
}

// Send attempts to push the events to the remote endpoint.
//
// On failure, the error is guaranteed to be one of: JSONError, RequestError, or
// UnexpectedStatusCodeError.
func Send[E Event](ctx context.Context, client Client, traceID TraceID, events []E) error {
	if len(events) == 0 {
		return nil
	}

	payload, err := json.Marshal(struct {
		Events []E `json:"events"`
	}{Events: events})
	if err != nil {
		return JSONError{Err: err}
	}

	return client.Send(ctx, payload, traceID)
}

type JSONError struct {
	Err error
}

func (e JSONError) Error() string {
	return fmt.Sprintf("Error marshaling events: %s", e.Err.Error())
}

func (e JSONError) Unwrap() error {
	return e.Err
}

type RequestError struct {
	Err error
}

func (e RequestError) Error() string {
	return fmt.Sprintf("Error making request: %s", e.Err.Error())
}

func (e RequestError) Unwrap() error {
	return e.Err
}

type UnexpectedStatusCodeError struct {
	StatusCode int
}

func (e UnexpectedStatusCodeError) Error() string {
	return fmt.Sprintf("Unexpected HTTP status code %d", e.StatusCode)
}
