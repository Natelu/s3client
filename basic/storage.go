package basic

import (
	"context"
	"errors"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
)

// Object contain content and metadata of S3 object.
type Object struct {
	Key                 *string                 `json:"-"`
	ETag                *string                 `json:"e_tag"`
	Mtime               *time.Time              `json:"mtime"`
	Content             *[]byte                 `json:"-"`
	ContentType         *string                 `json:"content_type"`
	ContentDisposition  *string                 `json:"content_disposition"`
	ContentEncoding     *string                 `json:"content_encoding"`
	ContentLanguage     *string                 `json:"content_language"`
	Metadata            map[string]*string      `json:"metadata"`
	ACL                 *string                 `json:"acl"`
	CacheControl        *string                 `json:"cache_control"`
	VersionId           *string                 `json:"version_id"`
	IsLatest            *bool                   `json:"-"`
	StorageClass        *string                 `json:"storage_class"`
	AccessControlPolicy *s3.AccessControlPolicy `json:"access_control_policy"`
}

// Storage interface.
type Storage interface {
	WithContext(ctx context.Context)
	WithRateLimit(limit int) error
	List(ch chan<- *Object) error
	PutObject(object *Object) error
	GetObjectContent(obj *Object) error
	GetObjectMeta(obj *Object) error
	GetObjectACL(obj *Object) error
	DeleteObject(obj *Object) error
}

// IsAwsContextCanceled ...
func IsAwsContextCanceled(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, context.Canceled) {
		return true
	}

	var aErr awserr.Error
	if ok := errors.As(err, &aErr); ok && aErr.OrigErr() == context.Canceled {
		return true
	} else if ok && aErr.Code() == request.CanceledErrorCode {
		return true
	}

	return false
}

// Save2LocalPath ... save Object to local path.
func Save2LocalPath(p string, obj *Object) {

}
