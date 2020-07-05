package httpcache

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	// TODO: pluggable local storage?
	// TODO: switch to https://github.com/dgraph-io/badger ?
	"github.com/golang/leveldb"
	leveldbdb "github.com/golang/leveldb/db"
)

// Return a net/http RoundTripper transport which caches GETs to local storage.
// If cachePath is "", returns http.DefaultTransport and no caching is done.
func NewCacheTransport(cachePath string) (rt http.RoundTripper, err error) {
	if len(cachePath) == 0 {
		return http.DefaultTransport, nil
	}
	db, err := leveldb.Open(cachePath, nil)
	if err != nil {
		return nil, fmt.Errorf("could not open http cache db, %v", err)
	}
	return &HttpCache{
		UnderlyingTransport: http.DefaultTransport,
		MaxBodyBytes:        1000000,
		db:                  db,
		cachePath:           cachePath,
	}, nil
}

func NewClient(cachePath string) (*http.Client, error) {
	if len(cachePath) == 0 {
		return http.DefaultClient, nil
	}
	rt, err := NewCacheTransport(cachePath)
	if err != nil {
		return nil, err
	}
	c := new(http.Client)
	c.Transport = rt
	return c, nil
}

// Implement net/http RoundTripper; store GET results to local storage.
//
// There's no maximum size of total cached stuff or LRU replacement.
// Everything is cached until it expires.
// This is simple and mostly useful for static-ish API objects? (Originally developed to hold some keys in an oauth process where the keys changed on a days-to-weeks rate.)
//
// TODO: max total size; LRU replacement (OR, closest-to-expiration replacement)
type HttpCache struct {
	// UnderlyingTransport defaults to http.DefaultTransport
	UnderlyingTransport http.RoundTripper

	// Maximum number of body bytes that will be saved to cache.
	// Defaults to 1000000.
	// Body gets read into RAM in its entirety whatever the size.
	// Larger things pass through but are not written to cache.
	MaxBodyBytes int

	db        *leveldb.DB
	cachePath string
}

// Returns nil, nil to signal caller to fall back to network and store result.
func (hc *HttpCache) roundTripTryCache(key []byte) (*http.Response, error) {
	if hc.db == nil {
		// this database failed, so cache is disabled
		return nil, nil
	}
	data, err := hc.db.Get(key, nil)
	if err == leveldbdb.ErrNotFound {
		// ok, just a cache miss
		return nil, nil
	} else if err != nil {
		log.Print("error reading http cache db, disabling cache ", err)
		// the database failed, give up on it for now and go straight to net
		hc.db = nil
		return nil, nil
	}
	if (data == nil) || (len(data) == 0) {
		// no cached value, go to net, maybe store result
		return nil, nil
	}
	expires, bytesused := binary.Varint(data)
	if bytesused <= 0 {
		log.Print("error reading cache timoeut ", bytesused)
		// delete bad stored result, go to net, store result
		hc.db.Delete(key, nil)
		return nil, nil
	}
	now := time.Now().Unix()
	if expires > now {
		sr := StoredResponse{}
		err = json.Unmarshal(data[bytesused:], &sr)
		if err != nil {
			log.Print("error unpacking response json ", err)
			// delete bad stored result, go to net, store result
			hc.db.Delete(key, nil)
			return nil, nil
		}
		br := strings.NewReader(sr.Body)
		response := &http.Response{
			Status:           sr.Status,
			StatusCode:       sr.StatusCode,
			Proto:            sr.Proto,
			ProtoMajor:       sr.ProtoMajor,
			ProtoMinor:       sr.ProtoMinor,
			Header:           sr.Headers,
			Body:             ioutil.NopCloser(br),
			TransferEncoding: sr.TransferEncoding,
			Trailer:          sr.Trailers,
		}
		//log.Print("cache hit ", string(key))
		return response, nil
	}
	//log.Print("cache stale ", string(key))
	// stale result, delete, go back to net, store result
	hc.db.Delete(key, nil)
	return nil, nil
}

func readBody(response *http.Response) ([]byte, error) {
	if response.ContentLength >= 0 {
		//log.Printf("reading %d from %#v", response.ContentLength, response.Body)
		var pos int64 = 0
		data := make([]byte, response.ContentLength)
		if response.ContentLength > 0 {
			for pos < response.ContentLength {
				len, err := response.Body.Read(data[pos:])
				if err == io.EOF {
					// ok
					response.Body.Close()
					pos += int64(len)
					if pos != response.ContentLength {
						log.Printf("wanted %d but got %d", response.ContentLength, pos)
					}
					return data[:pos], nil
				} else if err != nil {
					response.Body.Close()
					return nil, err
				}
				pos += int64(len)
			}
		}
		response.Body.Close()
		return data, nil
	} else {
		//log.Print("reading body till done")
		data, err := ioutil.ReadAll(response.Body)
		response.Body.Close()
		return data, err
	}
}

var maxAgeRe *regexp.Regexp = nil

func init() {
	maxAgeRe = regexp.MustCompile(`max-age=(\d+)`)
}

// Parse "Cache-Control" header.
// Return unix seconds time at which item expires.
// Return 0 for not-cacheable.
func CacheExpirationTime(response *http.Response) int64 {
	cc := response.Header.Get("Cache-Control")
	if len(cc) == 0 {
		return time.Now().Unix() + 3600
	}
	if strings.Contains(cc, "no-cache") {
		return 0
	}
	if strings.Contains(cc, "no-store") {
		return 0
	}
	matches := maxAgeRe.FindStringSubmatch(cc)
	if len(matches) > 1 {
		maxAge, err := strconv.ParseInt(matches[1], 10, 64)
		if err != nil {
			log.Print("bad max-age in ", cc)
			return 0
		}
		ageStr := response.Header.Get("Age")
		var age int64
		if len(ageStr) == 0 {
			age = 0
		} else {
			age, err = strconv.ParseInt(ageStr, 10, 64)
			if err != nil {
				log.Print("bad Age ", ageStr)
				age = 0
			}
		}
		return time.Now().Unix() + maxAge - age
	}
	return 0
}

// implement net/http RoundTripper
func (hc *HttpCache) RoundTrip(request *http.Request) (*http.Response, error) {
	// only cache GET
	if request.Method != "GET" {
		return hc.UnderlyingTransport.RoundTrip(request)
	}

	// try the cache
	key := []byte(request.URL.String())
	response, err := hc.roundTripTryCache(key)
	if (response != nil) || (err != nil) {
		// got either data or an error
		//log.Print("got cache for ", string(key))
		return response, err
	}

	// go to the net, maybe store result
	response, err = hc.UnderlyingTransport.RoundTrip(request)
	if response.StatusCode == 200 {
		// only cache good responses
		var expires int64 = CacheExpirationTime(response)
		if expires == 0 {
			log.Print("not cacheable: ", string(key))
			// not cacheable
			return response, err
		}
		bodyAll, err := readBody(response)
		if err != nil {
			//log.Print("readBody fail ", err)
			return nil, err
		}
		if len(bodyAll) < hc.MaxBodyBytes {
			bas := string(bodyAll)
			//log.Printf("read body cl=%d l=%d d=%#v", request.ContentLength, len(bodyAll), bas)
			sr := &StoredResponse{
				Status:           response.Status,
				StatusCode:       response.StatusCode,
				Proto:            response.Proto,
				ProtoMajor:       response.ProtoMajor,
				ProtoMinor:       response.ProtoMinor,
				Headers:          response.Header,
				Body:             bas,
				TransferEncoding: request.TransferEncoding,
				Trailers:         response.Trailer,
			}
			exdata := make([]byte, 10)
			opos := binary.PutVarint(exdata, expires)
			//srdata, err := proto.Marshal(sr)
			srdata, err := json.Marshal(sr)
			allout := make([]byte, opos+len(srdata))
			copy(allout, exdata[:opos])
			copy(allout[opos:], srdata)
			err = hc.db.Set(key, allout, nil)
			if err != nil {
				log.Print("failed to store to db ", key, " ", err)
			} else {
				//log.Print("response cached for ", string(key))
			}
		}
		br := bytes.NewReader(bodyAll)
		response.Body = ioutil.NopCloser(br)
	}
	return response, err
}

// database values are [varint expiration unix time seconds][json StoredResponse]
type StoredResponse struct {
	Status     string `json:"S"` // "200 OK"
	StatusCode int    `json:"s"` // 200
	Proto      string `json:"p"` // "HTTP/1.0"
	ProtoMajor int    `json:"P"` // 1
	ProtoMinor int    `json:"r"` // 0

	Headers          map[string][]string `json:"h,omitempty"`
	Body             string              `json:"b"`
	TransferEncoding []string            `json:"t,omitempty"`
	Trailers         map[string][]string `json:"z,omitempty"`
}

/// TODO: it is conceivable to write a big automated test for this that includes an HTTPServer and responds with varying content and caching headers and checks that a response that should be cached _doesn't_ hit the server, but that's a lot of work and I donwanna and in practice this thing works well enough for me.
