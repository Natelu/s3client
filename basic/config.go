package basic

import "github.com/aws/aws-sdk-go/aws/credentials"

var (
	// SessionRegion ... region name
	SessionRegion = "gz-tst"
	// SessionURL ... s3 connection url
	SessionURL = "cos.gz-tst.cos.tg.unicom.local"
	// AccessKey ... s3 tracsaction key
	AccessKey = "EDEHBIZHAPTBR85JBXZY"
	// SecretKey ... s3 tracsaction key
	SecretKey = "g0fnOS2MIGSZ5QBcQcSZWmVeMY3PCbrUGBJv0yIX"
)

// CKEProvider ...
type CKEProvider struct {
	Value credentials.Value
}

// Retrieve ...
func (cp *CKEProvider) Retrieve() (credentials.Value, error) {
	cp.Value = credentials.Value{
		AccessKeyID:     AccessKey,
		SecretAccessKey: SecretKey,
	}
	return cp.Value, nil
}

// IsExpired ...
func (cp *CKEProvider) IsExpired() bool {
	return false
}
