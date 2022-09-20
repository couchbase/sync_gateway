package main

import (
	"bytes"
	"crypto/sha1"
	"errors"
	"io"
)

type CopyFunc func(io.Writer, io.Reader) (int64, error)

func Copier(opts *SGCollectOptions) CopyFunc {
	if opts.LogRedactionLevel == RedactNone {
		return io.Copy
	}
	// implementation of io.Copy that also redacts UD data
	return func(dst io.Writer, src io.Reader) (int64, error) {
		var written int64
		var err error
		const maxRedactedDataSize = 64 * 1024 // 64MB
		size := 32 * 1024
		if l, ok := src.(*io.LimitedReader); ok && int64(size) > l.N {
			if l.N < 1 {
				size = 1
			} else {
				size = int(l.N)
			}
		}
		buf := make([]byte, size)
		redactBuf := make([]byte, 0, maxRedactedDataSize)
		for {
			numRead, readErr := src.Read(buf)
			var writeBuf []byte
			if numRead > 0 {
				var redacted []byte
				var needMore bool
				for {
					redactBuf = append(redactBuf, buf[:numRead]...)
					redacted, needMore = maybeRedactBuffer(redactBuf, []byte(opts.LogRedactionSalt))
					if !needMore {
						writeBuf = redacted
						redactBuf = make([]byte, 0, maxRedactedDataSize)
						break
					}
					numRead, readErr = src.Read(buf)
					if numRead > 0 {
						continue
					}
					if readErr != nil {
						goto ReadErr
					}
				}

				numWritten, writeErr := dst.Write(writeBuf)
				if numWritten < 0 || numRead < numWritten {
					numWritten = 0
					if writeErr == nil {
						writeErr = errors.New("invalid write result")
					}
				}
				written += int64(numWritten)
				if writeErr != nil {
					err = writeErr
					break
				}
				if numRead != numWritten {
					err = errors.New("short write")
					break
				}
			}
		ReadErr:
			if readErr != nil {
				if readErr != io.EOF {
					err = readErr
				}
				break
			}
		}
		return written, err
	}
}

// maybeRedactBuffer searches the given buffer for a redacted chunk (data wrapped in <ud></ud> tags). If it finds one,
// it returns a copy of buf with the contents redacted. If it finds an opening tag, but no closing tag, it returns
// needMore=true, in this case the caller should call it again with more data (with the same starting position).
// Note that only the first redacted string in the buffer will be redacted.
func maybeRedactBuffer(buf []byte, salt []byte) (newBuf []byte, needMore bool) {
	const startingTag = "<ud>"
	const endingTag = "</ud>"

	redactStartPos := bytes.Index(buf, []byte(startingTag))
	if redactStartPos == -1 {
		return buf, false
	}
	var beforeRedactBuf, redactBuf, afterRedactBuf []byte
	beforeRedactBuf = buf[0:redactStartPos]

	const startingTagLen = len(startingTag)
	const endingTagLen = len(endingTag)

	// This handles cases like <ud><ud>stuff</ud></ud> - we want the outermost tags to be redacted
	depth := 1
	for i := redactStartPos + startingTagLen; i < len(buf)-(startingTagLen+1); i++ {
		if bytes.Equal(buf[i:i+startingTagLen+1], []byte(startingTag)) {
			depth++
			continue
		}
		if bytes.Equal(buf[i:i+endingTagLen+1], []byte(endingTag)) {
			depth--
			if depth == 0 {
				beforeRedactBuf = buf[0:redactStartPos]
				redactBuf = buf[redactStartPos+1 : i-1]
				afterRedactBuf = buf[i+endingTagLen:]
				redacted := sha1.Sum(append(salt, redactBuf...))
				return append(append(beforeRedactBuf, redacted[:]...), afterRedactBuf...), false
			}
		}
	}
	if depth > 0 {
		// We've seen an opening redact tag, but not a closing redact tag.
		return nil, true
	}
	panic("unreachable")
}
