package base

const (

	// The username of the special "GUEST" user
	GuestUsername = "GUEST"
	ISO8601Format = "2006-01-02T15:04:05.000Z07:00"

	//kTestURL = "http://localhost:8091"
	kTestURL = "walrus:"

	kTestUseAuthHandler = false
	kTestUsername       = "sync_gateway_tests"
	kTestPassword       = "password"
	kTestBucketname     = "sync_gateway_tests"
)

func UnitTestUrl() string {
	return kTestURL
}

func UnitTestAuthHandler() AuthHandler {
	if !kTestUseAuthHandler {
		return nil
	} else {
		return &UnitTestAuth{
			username:   kTestUsername,
			password:   kTestPassword,
			bucketname: kTestBucketname,
		}
	}
}

type UnitTestAuth struct {
	username   string
	password   string
	bucketname string
}

func (u *UnitTestAuth) GetCredentials() (string, string, string) {
	return TransformBucketCredentials(u.username, u.password, u.bucketname)
}
