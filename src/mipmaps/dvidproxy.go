package mipmaps

import (
	"bytes"
	"fmt"
	"github.com/julienschmidt/httprouter"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"

	"config"
)

type dvidproxy struct {
	// name - instance name
	name string
	// dvid - dvid connection parameters (host:port)
	dvidConn string
	// dvidKVStoreConn - dvid KV store connection parameters (host:port)
	dvidKVStoreConn string
	httpImpl        *http.Server
	httpListener    net.Listener
	store           tileStore
}

// DVIDProxyURLMapping mapping of DVID proxy urls to DVID service http urls
type DVIDProxyURLMapping map[string]string

// formatRootURL converts a DVID url (one that has a dvid scheme, i.e., starts with dvid://) to an http url
func (dpm DVIDProxyURLMapping) formatRootURL(url string) string {
	if strings.HasPrefix(url, "dvid://") {
		dvidInstance := url[len("dvid://"):]
		if sepIndex := strings.Index(dvidInstance, "/"); sepIndex != -1 {
			dvidInstance = dvidInstance[0:sepIndex]
		}
		dvidProxyURL := dpm[dvidInstance]
		if dvidProxyURL != "" {
			return strings.Replace(url, "dvid://"+dvidInstance, dvidProxyURL, 1)
		}
		return strings.Replace(url, "dvid://", "http://", 1)
	}
	return url
}

// StartDVIDProxies start DVID proxies
func StartDVIDProxies(cfg config.Config) (DVIDProxyURLMapping, error) {
	dvidProxiesMap := DVIDProxyURLMapping{}

	log.Printf("Start DVID Proxies ...")
	dvids := getDvidProxies(cfg)
	for _, dvid := range dvids {
		httpListener, err := newLocalTCPListener()
		if err != nil {
			log.Printf("Error creating a DVID proxy for %v: %v", dvid, err)
			continue
		}
		dvid.initTileStore(cfg)
		dvid.setupDVIDProxyRouter()
		if err = dvid.startDVIDProxy(httpListener); err == nil {
			// in case of success
			dvidProxiesMap[dvid.name] = "http://" + httpListener.Addr().String() + dvidImageTileAPIBase
		}
	}
	return dvidProxiesMap, nil
}

func newLocalTCPListener() (net.Listener, error) {
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		log.Printf("Error creating the tcp4 listener: %v", err)
		if l, err = net.Listen("tcp6", "[::1]:0"); err != nil {
			return l, err
		}
		return l, nil
	}
	return l, nil
}

func (dp *dvidproxy) initTileStore(cfg config.Config) {
	dp.store = newKVScalityStore(dp.dvidConn, dp.dvidKVStoreConn, cfg.GetStringMapProperty("scalityRingsByCollection"))
}

func (dp *dvidproxy) setupDVIDProxyRouter() {
	router := httprouter.New()
	dp.httpImpl = &http.Server{Handler: router}
	dp.httpImpl.SetKeepAlivesEnabled(false)

	router.GET("/api/node/:owner/:storeName/:nodeUID/:dataInstance/:orientation/:scale/:xcoord/:ycoord/:zcoord", dp.retrieveTile)
	router.POST("/api/node/:owner/:storeName/:nodeUID/:dataInstance/:orientation/:scale/:xcoord/:ycoord/:zcoord", dp.storeTile)
}

func (dp *dvidproxy) startDVIDProxy(l net.Listener) error {
	errCh := make(chan error)
	log.Printf("Starting a DVID proxy for %v on %v", dp.dvidConn, l.Addr())
	go func(ech chan error) {
		err := dp.httpImpl.Serve(l)
		ech <- err
	}(errCh)
	select {
	case err := <-errCh:
		log.Printf("Failed to start a DVID proxy for %v on %v: %v", dp.dvidConn, l.Addr(), err)
		return err
	default:
		log.Printf("DVID proxy %v listening on %v", dp.dvidConn, l.Addr())
		dp.httpListener = l
		return nil
	}
}

func (dp *dvidproxy) stopDVIDProxy() error {
	return dp.httpListener.Close()
}

func (dp *dvidproxy) retrieveTile(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	t, err := extractDvidTile(r, params)
	if err != nil {
		log.Print(err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err = dp.store.generateTileKey(t); err != nil {
		log.Print(err)
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	if responseStatus, err := dp.store.retrieveTileContent(t); err != nil {
		log.Print(err)
		if responseStatus < http.StatusBadRequest {
			responseStatus = http.StatusBadGateway
		}
		http.Error(w, err.Error(), responseStatus)
		return
	}
	if _, err = w.Write(t.content); err != nil {
		log.Print(err)
	}
}

func (dp *dvidproxy) storeTile(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	t, err := extractDvidTile(r, params)
	if err != nil {
		log.Print(err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err = extractTileImage(t, r); err != nil {
		log.Print(err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	storeTileMethod := decorateTileFunc(dp.store.storeTile, retryProcessing(5))

	if err = storeTileMethod(t); err != nil {
		log.Print(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func extractDvidTile(r *http.Request, params httprouter.Params) (*dvidTile, error) {
	var err error
	var scale, xcoord, ycoord, zcoord uint64
	var orientation orientation
	owner := params.ByName("owner")
	storeName := params.ByName("storeName")
	nodeUID := params.ByName("nodeUID")
	dataInstance := params.ByName("dataInstance")
	orientation.Set(params.ByName("orientation"))
	if scale, err = strconv.ParseUint(params.ByName("scale"), 10, 16); err != nil {
		return nil, fmt.Errorf("Error parsing the scale parameter %s: %v", params.ByName("scale"), err)
	}
	if xcoord, err = strconv.ParseUint(params.ByName("xcoord"), 10, 32); err != nil {
		return nil, fmt.Errorf("Error parsing the tile col parameter %s: %v", params.ByName("col"), err)
	}
	if ycoord, err = strconv.ParseUint(params.ByName("ycoord"), 10, 32); err != nil {
		return nil, fmt.Errorf("Error parsing the tile row parameter %s: %v", params.ByName("row"), err)
	}
	if zcoord, err = strconv.ParseUint(params.ByName("zcoord"), 10, 32); err != nil {
		return nil, fmt.Errorf("Error parsing the tile layer parameter %s: %v", params.ByName("layer"), err)
	}
	return &dvidTile{
		owner:        owner,
		storeName:    storeName,
		nodeUID:      nodeUID,
		dataInstance: dataInstance,
		orientation:  orientation,
		scale:        uint8(scale),
		xcoord:       uint32(xcoord),
		ycoord:       uint32(ycoord),
		zcoord:       uint32(zcoord),
	}, nil
}

func extractTileImage(t *dvidTile, r *http.Request) (err error) {
	var contentBuffer bytes.Buffer
	var nbytes int64
	if nbytes, err = io.Copy(&contentBuffer, r.Body); err != nil {
		return fmt.Errorf("Error reading the request body: %v", err)
	}
	t.content = contentBuffer.Bytes()
	t.contentLength = nbytes
	return nil
}

func getDvidProxies(cfg config.Config) []*dvidproxy {
	var dvids []*dvidproxy
	if cfg["dvidinstances"] == nil || len(cfg["dvidinstances"].([]interface{})) == 0 {
		return dvids
	}
	dvidInstanceList := cfg["dvidinstances"].([]interface{})
	for _, dvidInstance := range dvidInstanceList {
		dvid := &dvidproxy{}
		dvidParams := dvidInstance.(map[string]interface{})
		dvid.name = dvidParams["name"].(string)
		dvid.dvidConn = dvidParams["dvid"].(string)
		dvid.dvidKVStoreConn = dvidParams["dvid-kv-store"].(string)
		dvids = append(dvids, dvid)
	}
	return dvids
}
