/// TODO: it is conceivable to write a big automated test for this that includes an HTTPServer and responds with varying content and caching headers and checks that a response that should be cached _doesn't_ hit the server, but that's a lot of work and I donwanna and in practice this thing works well enough for me.
package httpcache

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	// TODO: pluggable local storage?
	"github.com/golang/leveldb"
	leveldbdb "github.com/golang/leveldb/db"
)

/// Set this to enable GetHttpCache to use one app-wide http cache file
/// e.g. flag.StringVar(&httpcache.GlobalCachePath, "cachepath", "", "path to http client cache file")
var GlobalCachePath string = ""
var globalHttpCacheInstance *HttpCache = nil
var client *http.Client

func GetHttpCache() http.RoundTripper {
	if globalHttpCacheInstance != nil {
		return globalHttpCacheInstance
	}
	if len(GlobalCachePath) == 0 {
		return http.DefaultTransport
	}
	if globalHttpCacheInstance == nil {
		//log.Print("building cache roundtripper")
		db, err := leveldb.Open(GlobalCachePath, nil)
		if err != nil {
			log.Print("could not open http cache db ", err)
			return http.DefaultTransport
		}
		globalHttpCacheInstance = &HttpCache{
			http.DefaultTransport,
			db,
		}
	}
	return globalHttpCacheInstance
}

func GetClient() *http.Client {
	if client != nil {
		return client
	}
	rt := GetHttpCache()
	if rt == http.DefaultClient.Transport {
		return http.DefaultClient
	}
	if client == nil {
		//log.Print("building cache client")
		var tc http.Client = *http.DefaultClient
		client = &tc
		client.Transport = rt
	}
	return client
}

// implement net/http RoundTripper
type HttpCache struct {
	defaultrt http.RoundTripper
	db        *leveldb.DB
}

type XR struct {
	strings.Reader
}

func (xr *XR) Close() error {
	return nil
}

// Returns nil, nil to signal caller to fall back to network and store result.
func (hc *HttpCache) RoundTripTryCache(key []byte) (*http.Response, error) {
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
		/*
			err = proto.Unmarshal(data[bytesused:], &sr)
			if err != nil {
				log.Print("error unpacking response proto ", err)
				// delete bad stored result, go to net, store result
				hc.db.Delete(key, nil)
				return nil, nil
			}
		*/
		//br := bytes.NewReader([]byte(*sr.Body))
		br := strings.NewReader(sr.Body)
		//xr := &XR{*br}
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

// return 0 for not-cacheable
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

func (hc *HttpCache) RoundTrip(request *http.Request) (*http.Response, error) {
	// only cache GET
	if request.Method != "GET" {
		return hc.defaultrt.RoundTrip(request)
	}

	// try the cache
	key := []byte(request.URL.String())
	response, err := hc.RoundTripTryCache(key)
	if (response != nil) || (err != nil) {
		// got either data or an error
		//log.Print("got cache for ", string(key))
		return response, err
	}

	// go to the net, maybe store result
	response, err = hc.defaultrt.RoundTrip(request)
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
		br := bytes.NewReader(bodyAll)
		response.Body = ioutil.NopCloser(br)
	}
	return response, err
}

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
