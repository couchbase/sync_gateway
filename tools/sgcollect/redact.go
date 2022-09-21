package main

import (
	"bufio"
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"io"
	"log"
)

// CopyFunc is the signature of io.Copy.
type CopyFunc func(io.Writer, io.Reader) (int64, error)

// Copier returns a CopyFunc appropriate for the configured redaction level.
func Copier(opts *SGCollectOptions) CopyFunc {
	if opts.LogRedactionLevel == RedactNone {
		return io.Copy
	}
	// implementation of io.Copy that also redacts UD data
	return func(dst io.Writer, src io.Reader) (int64, error) {
		var written int64
		var err error

		flush := func(chunk []byte) error {
			nw, wErr := dst.Write(chunk)
			if nw < 0 || nw < len(chunk) {
				nw = 0
				if wErr == nil {
					wErr = errors.New("invalid write")
				}
			}
			written += int64(nw)
			if wErr != nil {
				return wErr
			}
			if errors.Is(err, io.EOF) {
				err = nil // match the io.Copy protocol
			}
			if len(chunk) != nw {
				return errors.New("short write")
			}
			return nil
		}

		br := bufio.NewReader(src)
		var tmp []byte
		redactBuf := make([]byte, 0, 32*1024)
		depth := 0
		for {
			chunk, readErr := br.ReadBytes('<')
			if errors.Is(readErr, io.EOF) {
				if depth > 0 {
					log.Println("WARN: mismatched UD tag")
					err = flush(append([]byte("<ud>"), chunk...))
				} else {
					err = flush(chunk)
				}
				break
			}
			if readErr != nil {
				err = readErr
				break
			}
			// Check if the next tag is an opening or closing tag.
			tmp, err = br.Peek(4)
			if err != nil {
				if errors.Is(err, io.EOF) {
					log.Printf("WARN: Corrupt redaction tag")
					err = flush(chunk)
					if err != nil {
						break
					}
					continue
				}
				err = readErr
				break
			}
			if string(tmp[:3]) == "ud>" {
				// opening
				if depth == 0 {
					// need to first write out everything up to the opening <
					err = flush(chunk[:len(chunk)-1])
					if err != nil {
						break
					}
					// and then discard the remainder of the opening tag, as it doesn't get redacted (its contents do)
					_, err = br.Discard(3)
					if err != nil {
						err = readErr
						break
					}
					// now the br is just after the opening <ud>
				} else {
					// need to push the entire chunk into the redact buffer, *including* this opening <ud> because it's nested
					redactBuf = append(redactBuf, chunk...)
				}
				depth++
				// continue reading until we either hit the end of the source or find the closing UD
				continue
			} else if string(tmp[:4]) == "/ud>" {
				// closing
				depth--
				if depth == 0 {
					// chunk will now be the complete redactable area, because we discard everything up to it, plus the
					// closing >.
					_, err = br.Discard(4)
					if err != nil {
						err = readErr
						break
					}
					// now the br is just after the closing </ud>>
					redactBuf = append(redactBuf, chunk[:len(chunk)-1]...)
					sumInput := append([]byte(opts.LogRedactionSalt), redactBuf...)
					digest := sha1.Sum(sumInput)
					chunk = append(append([]byte("<ud>"), hex.EncodeToString(digest[:])...), []byte("</ud>")...)
					redactBuf = make([]byte, 0, 32*1024)
				}
			}
			// it's not an opening tag, either it's a closing tag or not a tag we care about
			// if we're inside a redaction tag, it needs to get added to the redaction buffer, otherwise it can go
			// out as it is
			if depth > 0 {
				redactBuf = append(redactBuf, chunk...)
			} else {
				err = flush(chunk)
				if err != nil {
					break
				}
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
