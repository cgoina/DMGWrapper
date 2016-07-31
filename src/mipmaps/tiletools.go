package mipmaps

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
)

const (
	dvidImageTileAPIBase = "/api/node"
	kvStoreAPIBase       = "/kvautobus/api/key/"
)

type dvidTile struct {
	owner, storeName       string
	nodeUID, dataInstance  string
	tileKey                string
	orientation            orientation
	scale                  uint8
	xcoord, ycoord, zcoord uint32
	content, checksum      []byte
	contentLength          int64
}

// String format the dvidTile
func (t dvidTile) String() string {
	return fmt.Sprintf("%s/%s/%s/%s/%s/%d/%d_%d_%d",
		t.owner, t.storeName, t.nodeUID, t.dataInstance,
		t.orientation.formatDVID(), t.scale, t.xcoord, t.ycoord, t.zcoord)
}

// tileStore service
type tileStore interface {
	generateTileKey(t *dvidTile) error
	storeTileMetadata(t *dvidTile) error
	retrieveTileContent(t *dvidTile) (int, error)
	storeTileContent(t *dvidTile) error
	storeTile(t *dvidTile) error
}

type kvScalityStore struct {
	dvidConn, kvStoreConn    string
	scalityRingsByCollection map[string]string
}

type tileFunc func(t *dvidTile) error

func (tf tileFunc) apply(t *dvidTile) error {
	return tf(t)
}

type tileFuncDecorator func(tf tileFunc) tileFunc

// decorateTileFunc - decorate a tile method
func decorateTileFunc(tf tileFunc, decorators ...tileFuncDecorator) tileFunc {
	decorated := tf
	for _, decorate := range decorators {
		decorated = decorate(decorated)
	}
	return decorated
}

func newKVScalityStore(dvidConn, kvStoreConn string, scalityRingsByCollection map[string]string) tileStore {
	log.Printf("Create KV Scality store: %s, %s, %v\n", dvidConn, kvStoreConn, scalityRingsByCollection)
	return &kvScalityStore{
		dvidConn:                 dvidConn,
		kvStoreConn:              kvStoreConn,
		scalityRingsByCollection: scalityRingsByCollection,
	}
}

func (kvs *kvScalityStore) generateTileKey(t *dvidTile) (err error) {
	tileKeyURL := kvs.dvidConn +
		dvidImageTileAPIBase +
		fmt.Sprintf("/%s/%s/tilekey/%s/%d/%d_%d_%d", t.nodeUID, t.dataInstance, t.orientation.formatDVID(), t.scale, t.xcoord, t.ycoord, t.zcoord)
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("Error while generating the key for %s: %v", tileKeyURL, r)
		}
	}()
	resp, err := http.Get(tileKeyURL)
	if err != nil {
		return fmt.Errorf("Error getting tile key from %s: %v", tileKeyURL, err)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("Error reading body response for %s (%d): %s - %s", tileKeyURL, resp.StatusCode, body, err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Error getting tile key from %s (%d): %s", tileKeyURL, resp.StatusCode, body)
	}
	var tileKeyInfo map[string]interface{}
	err = json.Unmarshal(body, &tileKeyInfo)
	if err != nil {
		return fmt.Errorf("Error decoding response for %s - %s: %v", tileKeyURL, body, err)
	}
	tileKeyBytes, err := hex.DecodeString(tileKeyInfo["key"].(string))
	if err != nil {
		return fmt.Errorf("Error decoding key for %s from %s: %v", tileKeyURL, body, err)
	}
	t.tileKey = base64.URLEncoding.EncodeToString(tileKeyBytes)
	return nil
}

func (kvs *kvScalityStore) storeTileMetadata(t *dvidTile) (err error) {
	tileIndexURL := kvs.kvStoreConn + kvStoreAPIBase + t.storeName
	if t.checksum == nil {
		md5Checksum := md5.Sum(t.content)
		t.checksum = md5Checksum[0:]
	}

	tileIndexReq := map[string]interface{}{
		"key":         t.tileKey,
		"owner":       t.owner,
		"fileservice": t.storeName,
		"checksum":    hex.EncodeToString(t.checksum),
		"size":        t.contentLength,
	}
	tileIndexJSONReq, err := json.Marshal(tileIndexReq)
	if err != nil {
		return fmt.Errorf("Error encoding KV request: %v: %v", tileIndexReq, err)
	}
	resp, err := http.Post(tileIndexURL, "application/json", bytes.NewReader(tileIndexJSONReq))
	if err != nil {
		return fmt.Errorf("Error sending KV request %s to %s: %v", tileIndexJSONReq, tileIndexURL, err)
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("Error reading response body from %s (%d): %v", tileIndexURL, resp.StatusCode, err)
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode == http.StatusOK:
		return nil
	case resp.StatusCode == http.StatusConflict:
		log.Printf("Tile key %s (%s) is already registered at %s", t.String(), t.tileKey, tileIndexURL)
		return nil
	default:
		return fmt.Errorf("Error (%d) encountered while posting %s (%s) to %s - %s",
			resp.StatusCode, t.String(), t.tileKey, tileIndexURL, body)
	}
}

func (kvs *kvScalityStore) retrieveTileContent(t *dvidTile) (code int, err error) {
	code = 0
	var scalityRingURL string
	if scalityRingURL, err = kvs.getScalityURL(t); err != nil {
		return code, err
	}
	scalityURL := scalityRingURL + "/" + t.tileKey

	resp, err := http.Get(scalityURL)
	if err != nil {
		return code, fmt.Errorf("Error getting tile %s (%s) content from %s: %v", t.String(), t.tileKey, scalityURL, err)
	}
	defer resp.Body.Close()
	code = resp.StatusCode
	if code != http.StatusOK {
		return code, fmt.Errorf("Read tile %s (%s) %s - status %d", t.String(), t.tileKey, scalityURL, code)
	}
	log.Printf("Found tile %s (%s) %s - status %d", t.String(), t.tileKey, scalityURL, code)
	if t.content, err = ioutil.ReadAll(resp.Body); err != nil {
		return code, fmt.Errorf("Read tile %s (%s) error : %v", t.String(), t.tileKey, err)
	}
	return code, nil
}

func (kvs *kvScalityStore) storeTileContent(t *dvidTile) (err error) {
	var r *http.Request
	var scalityRingURL string
	if scalityRingURL, err = kvs.getScalityURL(t); err != nil {
		return err
	}
	scalityURL := scalityRingURL + "/" + t.tileKey

	if r, err = http.NewRequest("PUT", scalityURL, bytes.NewReader(t.content)); err != nil {
		return fmt.Errorf("Error creating the request to store to %s (%s) at %s: %v", t.String(), t.tileKey, scalityURL, err)
	}
	r.Close = true
	httpClient := http.Client{}
	resp, err := httpClient.Do(r)
	if err != nil {
		return fmt.Errorf("Error sending the request to store to %s (%s) at %s: %v", t.String(), t.tileKey, scalityURL, err)
	}
	io.Copy(ioutil.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Store tile content for %s (%s) at %s return status code %d", t.String(), t.tileKey, scalityURL, resp.StatusCode)
	}
	log.Printf("Stored tile %s (%s) -> %s", t.String(), t.tileKey, scalityURL)
	return nil
}

func (kvs *kvScalityStore) getScalityURL(t *dvidTile) (scalityRingURL string, err error) {
	if scalityRingURL = kvs.scalityRingsByCollection[t.storeName]; scalityRingURL == "" {
		scalityRingURL = kvs.scalityRingsByCollection["default"]
	}
	if scalityRingURL == "" {
		return "", fmt.Errorf("Invalid data store collection or scalityRing configuration for storing %s (%s)",
			t.String(), t.tileKey)
	}
	return scalityRingURL, nil
}

func (kvs *kvScalityStore) storeTile(t *dvidTile) (err error) {
	if t.tileKey == "" {
		// no need to generate the key if the it is already set
		if err = kvs.generateTileKey(t); err != nil {
			return err
		}
	}
	if err = kvs.storeTileMetadata(t); err != nil {
		return err
	}
	if err = kvs.storeTileContent(t); err != nil {
		return err
	}
	return nil
}

// retryProcessing - decorator to retry to process a tile the specified number
// of times in case it fails.
func retryProcessing(maxRetries int) tileFuncDecorator {
	return func(tf tileFunc) tileFunc {
		return tileFunc(func(t *dvidTile) error {
			res := tf(t)
			if res == nil {
				return res
			}
			log.Print(res)
			for r := 1; r < maxRetries; r++ {
				log.Printf("Retry (%d) processing  for %s", r, t.String())
				res = tf(t)
				if res == nil {
					return res
				}
				log.Print(res)
			}
			return fmt.Errorf("Processing of %s aborted after %d retries", t.String(), maxRetries)
		})
	}
}
