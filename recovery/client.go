package main

import (
	"bytes"
	"context"
	"io"
	"log"
	"net/url"
	"s3Client/recovery/basic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/larrabee/ratelimit"
)

// S3Client ...
type S3Client struct {
	Svc           *s3.S3
	Bucket        *string
	retryCnt      uint
	retryInterval time.Duration
	ctx           context.Context
	listMaker     *string
	rlBucket      ratelimit.Bucket
	prefix        *string
	keysPerReq    int64
}

// NewS3Client ...
func NewS3Client(bucket string, ctx context.Context) *S3Client {
	cred := credentials.NewCredentials(&basic.CKEProvider{})
	sess := session.Must(session.NewSession(&aws.Config{
		Region:                        &basic.SessionRegion,
		Endpoint:                      &basic.SessionURL,
		CredentialsChainVerboseErrors: aws.Bool(true),
		Credentials:                   cred,
		DisableSSL:                    aws.Bool(true),
	}))
	return &S3Client{
		Svc: s3.New(sess),
	}
}

// List ... list s3 bucket and send founded objects to chan
func (cli *S3Client) List(output chan<- *Object) error {
	listObjectsFn := func(p *s3.ListObjectsOutput, lastPage bool) bool {
		for _, obj := range p.Contents {
			key, _ := url.QueryUnescape(aws.StringValue(obj.Key))
			// key = strings.Replace(key)
			output <- &Object{
				Key:          aws.String(key),
				ETag:         obj.ETag,
				Mtime:        obj.LastModified,
				StorageClass: obj.StorageClass,
				IsLatest:     aws.Bool(true),
			}
		}
		cli.listMaker = p.Marker
		return !lastPage
	}

	for i := uint(0); ; i++ {
		input := &s3.ListObjectsInput{
			Bucket:       cli.Bucket,
			Prefix:       cli.prefix,
			MaxKeys:      &cli.keysPerReq,
			EncodingType: aws.String(s3.EncodingTypeUrl),
			Marker:       cli.listMaker,
		}
		err := cli.Svc.ListObjectsPagesWithContext(cli.ctx, input, listObjectsFn)
		if err == nil {
			log.Println("Listing bucket finished")
			return nil
		} else if IsAwsContextCanceled(err) {
			return err
		} else if (err != nil) && (i < cli.retryCnt) {
			log.Panicf("S3 listing failed with error: %s", err)
			time.Sleep(cli.retryInterval)
			continue
		} else if (err != nil) && (i == cli.retryCnt) {
			log.Panicf("S3 listing failed with error: %s", err)
			return err
		}
	}
}

// GetObjectContent ... read S3 object content and metadata from S3.
func (cli *S3Client) GetObjectContent(obj *Object) error {
	input := &s3.GetObjectInput{
		Bucket:    cli.Bucket,
		Key:       obj.Key,
		VersionId: obj.VersionId,
	}

	for i := uint(0); ; i++ {
		result, err := cli.Svc.GetObjectWithContext(cli.ctx, input)
		if IsAwsContextCanceled(err) {
			return err
		} else if (err != nil) && (i < cli.retryCnt) {
			log.Panicf("s3 obj content downloading request failed with error %s, %d th time", err, i)
			time.Sleep(cli.retryInterval)
			continue
		} else if (err != nil) && (i == cli.retryCnt) {
			return err
		}

		buf := bytes.NewBuffer(make([]byte, 0, aws.Int64Value(result.ContentLength)))
		_, err = io.Copy(ratelimit.NewWriter(buf, cli.rlBucket), result.Body)
		if IsAwsContextCanceled(err) {
			return err
		} else if (err != nil) && (i < cli.retryCnt) {
			log.Panicf("S3 object downloading failed with error : %s, ", err)
			time.Sleep(cli.retryInterval)
			continue
		} else if (err != nil) && (i == cli.retryCnt) {
			return err
		}
		data := buf.Bytes()
		obj.Content = &data
		obj.ContentType = result.ContentType
		obj.ContentDisposition = result.ContentDisposition
		obj.ETag = result.ETag
		obj.Metadata = result.Metadata
		obj.Mtime = result.LastModified
		obj.CacheControl = result.CacheControl
		obj.StorageClass = result.StorageClass

		return nil
	}
}

//PutObjectContent ... save object to S3 which will always ignore versionId and save objects as latest version.
func (cli *S3Client) PutObjectContent(obj *Object) error {
	objReader := bytes.NewReader(*obj.Content)
	rlReader := ratelimit.NewReadSeeker(objReader, cli.rlBucket)

	input := &s3.PutObjectInput{
		Bucket:             cli.Bucket,
		Key:                aws.String(*obj.Key),
		Body:               rlReader,
		ContentType:        obj.ContentType,
		ContentDisposition: obj.ContentDisposition,
		ContentLanguage:    obj.ContentLanguage,
		ContentEncoding:    obj.ContentEncoding,
		ACL:                obj.ACL,
		Metadata:           obj.Metadata,
		CacheControl:       obj.CacheControl,
		StorageClass:       obj.StorageClass,
	}

	for i := uint(0); ; i++ {
		_, err := cli.Svc.PutObjectWithContext(cli.ctx, input)
		if IsAwsContextCanceled(err) {
			return err
		} else if (err != nil) && (i < cli.retryCnt) {
			log.Panicf("s3 object uploading failed with error : %s .", err)
			time.Sleep(cli.retryInterval)
			continue
		} else if (err != nil) && (i == cli.retryCnt) {
			return err
		} else if err == nil {
			break
		}
	}

	if obj.AccessControlPolicy != nil {
		inputACL := &s3.PutObjectAclInput{
			Bucket:              cli.Bucket,
			Key:                 aws.String(*obj.Key),
			AccessControlPolicy: obj.AccessControlPolicy,
		}

		for i := uint(0); ; i++ {
			_, err := cli.Svc.PutObjectAclWithContext(cli.ctx, inputACL)
			if IsAwsContextCanceled(err) {
				return err
			} else if err == nil {
				break
			} else if (err != nil) && (i < cli.retryCnt) {
				log.Panicf("S3 ACL uploading failed with error: %s", err)
				time.Sleep(cli.retryInterval)
				continue
			} else if (err != nil) && (i == cli.retryCnt) {
				return err
			}
		}
	}

	return nil
}

func main() {
	// cli := NewS3Client()
	// bucket := "cke-backup"
	// 文件下载
}
