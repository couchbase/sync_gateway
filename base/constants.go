package base

const (

	// The username of the special "GUEST" user
	GuestUsername = "GUEST"
	ISO8601Format = "2006-01-02T15:04:05.000Z07:00"

	//kTestURL = "http://localhost:8091"
	kTestURL = "walrus:"
)

func UnitTestUrl() string {
	return kTestURL
}
