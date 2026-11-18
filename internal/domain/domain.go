package domain

import (
	"context"
	"errors"
	"io"
	"time"
)

var (
	ErrNotFound    = errors.New("avatar not found")
	ErrForbidden   = errors.New("you can only delete your own avatars")
	ErrInvalid     = errors.New("invalid request")
	ErrTooLarge    = errors.New("file too large")
	ErrUnsupported = errors.New("unsupported image format")
	ErrProcessing  = errors.New("thumbnail is still processing")
)

const MaxFileSize int64 = 10 * 1024 * 1024
const (
	EventUploaded    = "avatar.uploaded"
	EventDeleted     = "avatar.deleted"
	StatusProcessing = "processing"
	StatusCompleted  = "completed"
	StatusFailed     = "failed"
)

type Dimensions struct {
	Width  int `json:"width"`
	Height int `json:"height"`
}
type Thumbnail struct {
	Size  string `json:"size"`
	URL   string `json:"url"`
	S3Key string `json:"-"`
}
type Avatar struct {
	ID         string      `json:"id"`
	UserID     string      `json:"user_id"`
	FileName   string      `json:"file_name"`
	MIMEType   string      `json:"mime_type"`
	Size       int64       `json:"size"`
	S3Key      string      `json:"-"`
	Dimensions Dimensions  `json:"dimensions"`
	Thumbnails []Thumbnail `json:"thumbnails"`
	Status     string      `json:"status"`
	URL        string      `json:"url"`
	CreatedAt  time.Time   `json:"created_at"`
	UpdatedAt  time.Time   `json:"updated_at"`
	DeletedAt  *time.Time  `json:"-"`
}

type Event struct {
	ID        string    `json:"message_id"`
	Type      string    `json:"type"`
	AvatarID  string    `json:"avatar_id"`
	UserID    string    `json:"user_id"`
	S3Key     string    `json:"s3_key,omitempty"`
	S3Keys    []string  `json:"s3_keys,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

type JobResult struct {
	Thumbnails []Thumbnail
	Status     string
}
type EventHandler func(context.Context, Avatar) (JobResult, error)

type Repository interface {
	Create(context.Context, Avatar, Event) error
	Get(context.Context, string) (Avatar, error)
	Current(context.Context, string) (Avatar, error)
	List(context.Context, string) ([]Avatar, error)
	SoftDelete(context.Context, string, string, Event) error
	ProcessEvent(context.Context, Event, EventHandler) error
	Ping(context.Context) error
}

type Outbox interface {
	// DispatchPending holds a row lock until publish succeeds and marks sent in the
	// same transaction. Broker acknowledgement loss may cause duplicates by design.
	DispatchPending(context.Context, func(context.Context, Event) error) (bool, error)
}

type Storage interface {
	Put(context.Context, string, io.Reader, int64, string) error
	Get(context.Context, string) (io.ReadCloser, error)
	Delete(context.Context, string) error
	Ping(context.Context) error
}

type ImageInfo struct {
	MIMEType   string
	Dimensions Dimensions
}
type Variant struct {
	Size     string
	Data     []byte
	MIMEType string
}
type ImageProcessor interface {
	Inspect([]byte) (ImageInfo, error)
	Thumbnails([]byte) ([]Variant, error)
	Convert([]byte, string) ([]byte, string, error)
}

type Publisher interface {
	Publish(context.Context, Event) error
	Ping(context.Context) error
}
