# examples/ssl

This directory contains a self-signed SSL certificate (`cert.pem`, `privkey.pem`) for testing purposes. It's certainly not secure, since we've given away the private key!

There's also a simple Sync Gateway configuration file (`ssl.json`) that shows how to use the certificate. You just need to add two top-level keys:

    "SSLCert": "examples/ssl/cert.pem",
    "SSLKey":  "examples/ssl/privkey.pem",

Note that the Sync Gateway serves _only_ SSL when this is configured. If you want to support both SSL and plaintext connections, you'll need to run two instances of Sync Gateway, one with the SSL keys in its configuration and one without, and listening on different ports.

## How to make your own self-signed SSL cert

You probably don't want a self-signed certificate for public use, because clients can't verify its authenticity. Instead you should get a cert from a reputable Certificate Authority, which will be signed by that authority.

But if you _do_ want to make your own self-signed certificate, it's pretty easy using the `openssl` command-line tool and [these directions](https://www.openssl.org/docs/HOWTO/certificates.txt). In a nutshell, you just need to run these commands:

```
openssl genrsa -out privkey.pem 2048
openssl req -new -x509 -key privkey.pem -out cert.pem -days 1095
```

The second command is interactive and will ask you for information like country and city name that goes into the X.509 certificate. You can put whatever you want there; the only important part is the field `Common Name (e.g. server FQDN or YOUR name)` which needs to be the exact hostname that clients will reach your server at. The client will verify that this name matches the hostname in the URL it's trying to access, and will reject the connection if it doesn't.

You should now have two files:

* `privkey.pem`: the private key. **This needs to be kept secure** -- anyone who has this data can impersonate your server.
* `cert.pem`: the public certificate. You'll want to embed a copy of this in an application that connects to your server, so it can verify that it's actually connecting to your server and not some other server that also has a cert with the same hostname. The SSL client API you're using should have a function to either register a trusted 'root certificate', or to check whether two certificates have the same key.

Then just add the `"SSLCert"` and `"SSLKey"` properties to your Sync Gateway configuration file, as shown up above.