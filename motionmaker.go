package main

import (
	"encoding/json"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"time"

	// Deep AR pkg
	"github.com/amundsentech/convert"

	"github.com/golang/geo/s2"
	_ "github.com/lib/pq"
	"github.com/paulmach/go.geojson"
	"github.com/syndtr/goleveldb/leveldb"
)

const (
	currentleveldbpath = "/data/leveldb/"
	currentdb = "current"
	currentpoints = "circle_of_points.csv"

	//set the s2 cell parameters
	maxLevel = 10
	minLevel = 10
	maxCells = 10
)

type CParams struct {
	LevelDB   *leveldb.DB //the db connection
}

type Payload struct {
	Token []byte
	Value []byte
}

var (
	current CParams
)

/*
func main() {
	// creates a new leveldb conn in params.LevelDB
	err := LevelDBInit()
	if err != nil {
		return
	}

	defer params.LevelDB.Close()

	for {
		runagain()
	}
}
*/

func RunCurrent(working *leveldb.DB) {
	current.LevelDB = working
	fetched := fetchData()
	processData(fetched)
}

/*
func LevelDBInit() error {
	var err error

	params.TB = currentdb

	// make sure leveldb directory exists
	path := filepath.Join(currentleveldbpath)

	if _, err := os.Stat(path); os.IsNotExist(err) {
		os.Mkdir(path, os.ModePerm)
	}

	levelDBName := filepath.Join(currentleveldbpath, params.TB)

	// connect to new leveldb
	params.LevelDB, err = leveldb.OpenFile(levelDBName, nil)
	if err != nil {
		log.Printf("could not open/create leveldb :%s", err)
	}

	// DO NOT ADD defer params.LevelDB.Close() here
	// --> it will close the db prematurely when function returns

	return err
}
*/

func fetchData() <-chan convert.FeatureInfo {
	a := make(chan convert.FeatureInfo)

	// Need to wrap this in a go routine so a returns immediately and doesn't block
	go func() {

		for {

		// connect to and parse CSV of points
		csvfile, err := os.Open(currentpoints)
		if err != nil {
			log.Fatalln("Couldn't open the csv file", err)
		}

		// Parse the file
		r := csv.NewReader(csvfile)

			for {

				// Read each record from csv
				record, err := r.Read()
				if err == io.EOF {
					break
				}
				if err != nil {
					log.Fatal(err)
				}

				var feature convert.FeatureInfo
				//err = rows.Scan(&geomstr, &gfeaturestr)

				x, err := strconv.ParseFloat(record[0], 64)
				y, err := strconv.ParseFloat(record[1], 64)
				z, err := strconv.ParseFloat(record[2], 64)

				feature.GeomType = "Point"
				feature.SRID = "3857"
				feature.ID = "endless circle"

				point := []float64{x,y,z}

				fc := geojson.NewFeatureCollection()

				fc.AddFeature(geojson.NewPointFeature(point))

				geomstr, _ := json.Marshal(fc.Features[0].Geometry)

				feature.Geojson.Geometry, err = geojson.UnmarshalGeometry([]byte(geomstr))
				if err != nil {
					fmt.Printf("%v",err)
					break
				}

				getS2point(&feature)

				a <- feature
			}
		}
	}()

	return a
}


func processData(a <-chan convert.FeatureInfo) {
	var tokenswritten int

	//prepare each feature for leveldb
	for {

		feature, ok := <-a

		// end for loop if channel closes
		if !ok {
			log.Println("last feature received in channel")
			break
		}

		// create a new leveldb entry for each s2 key intersected
		for _, token := range feature.Tokens {
			payload := prepUNITYValue(&feature, []byte(token))

			// remove the existing value
			err := current.LevelDB.Delete(payload.Token, nil)
			if err != nil {
                                log.Printf("Could not delete, %v", payload.Token)
                                return
                        }

			_, err = current.LevelDB.Get(payload.Token,nil)
			if err != nil {
                                log.Printf("Key of %v successfully deleted", string(payload.Token))
                        }

			// put the mutated collection into the leveldb
			err = current.LevelDB.Put(payload.Token, payload.Value, nil)
			if err != nil {
				log.Printf("Could not write the key %v", payload.Token)
				return
			}

			log.Printf("Outbound dataset:\n%v",string(payload.Value))

			log.Printf("Key of %v successfull written",string(payload.Token))

			tokenswritten += 1
			if tokenswritten%10 == 0 {
				log.Printf("%v features written to db", tokenswritten)
			}

			time.Sleep(500000 * time.Microsecond)
		}
	}
	//close(b)
	//params.LevelDB.Close()
}

//prepUNITY prepares the key:value leveldb pair in the UNITY JSON / DEEP AR Format
func prepUNITYValue(feature *convert.FeatureInfo, token []byte) *Payload {

	var payload Payload

	// assign new, empty Dataset
	var indataset,outdataset convert.Datasets

	// is there already a cellection with the same s2 key?
	existing, _ := current.LevelDB.Get(token, nil)

	log.Printf("Inbound dataset:\n%v",string(existing))

	// if so, retain the information
	if len(existing) > 0 {
		err := json.Unmarshal(existing, &indataset)
		if err != nil {
			log.Printf("error unmarshaling existing leveldb to collection: %s", err.Error())
		}
	}

	//if a feature already exists with the ID, don't use it
	for _, test := range indataset.Points {
		if test.ID != feature.ID {
			log.Printf("test.ID '%s' does not match feature.ID '%s'",test.ID,feature.ID)
			outdataset.Points = append(outdataset.Points,test)
		}
	}

	//convert the feature into the UNITY JSON style
	convert.ParseGEOJSONFeature(feature, &outdataset, nil)

	collection, err := json.Marshal(outdataset)
	if err != nil {
		log.Println("%s", err.Error())
		return &payload
	}

	payload.Token = token
	payload.Value = collection

	return &payload
}

func getS2point (feature *convert.FeatureInfo) {
	var latlng s2.LatLng
	var pts []s2.Point

	x, y := convert.To4326(feature.Geojson.Geometry.Point[0], feature.Geojson.Geometry.Point[1])

	// add to s2 array of points
	latlng = s2.LatLngFromDegrees(y, x)
	pt := s2.PointFromLatLng(latlng)
	pts = append(pts, pt)

	// get s2 covering
	rc := &s2.RegionCoverer{MaxLevel: maxLevel, MinLevel: minLevel, MaxCells: maxCells}
	r := s2.Region(pts[0].CapBound())
	covering := rc.Covering(r)

	// append the s2 cells into the feature array, and populate the string tokens
	for _, c := range covering {
		feature.S2 = append(feature.S2, c)
		feature.Tokens = append(feature.Tokens, c.ToToken())
	}
}
