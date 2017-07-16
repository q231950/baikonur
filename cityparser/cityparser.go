package cityparser

import (
	"bytes"
	"encoding/csv"
	"html/template"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	log "github.com/apex/log"

	"github.com/q231950/baikonur/model"
	"github.com/q231950/sputnik/keymanager"
	requests "github.com/q231950/sputnik/requesthandling"
)

// CityParser parses cities out of a csv file
type CityParser struct {
}

// Parse parses the file given input
func (p CityParser) Parse(reader io.Reader) {
	r := csv.NewReader(reader)
	r.Comma = '\t'

	recordChannel := make(chan []string, 10)
	wg := new(sync.WaitGroup)

	go p.insertCity(recordChannel, wg)

	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			log.WithError(err).Fatal("Failed to read record from csv line")
		} else {
			wg.Add(1)
			sleepTimeMillis := time.Duration(1000 * time.Millisecond)
			log.Debugf("Sleeping for %s", sleepTimeMillis)
			time.Sleep(sleepTimeMillis)
			recordChannel <- record
		}
	}

	wg.Wait()

	close(recordChannel)
}

// The main 'geoname' table has the following fields :
// ---------------------------------------------------
// geonameid         : integer id of record in geonames database
// name              : name of geographical point (utf8) varchar(200)
// asciiname         : name of geographical point in plain ascii characters, varchar(200)
// alternatenames    : alternatenames, comma separated, ascii names automatically transliterated, convenience attribute from alternatename table, varchar(10000)
// latitude          : latitude in decimal degrees (wgs84)
// longitude         : longitude in decimal degrees (wgs84)
// feature class     : see http://www.geonames.org/export/codes.html, char(1)
// feature code      : see http://www.geonames.org/export/codes.html, varchar(10)
// country code      : ISO-3166 2-letter country code, 2 characters
// cc2               : alternate country codes, comma separated, ISO-3166 2-letter country code, 200 characters
// admin1 code       : fipscode (subject to change to iso code), see exceptions below, see file admin1Codes.txt for display names of this code; varchar(20)
// admin2 code       : code for the second administrative division, a county in the US, see file admin2Codes.txt; varchar(80)
// admin3 code       : code for third level administrative division, varchar(20)
// admin4 code       : code for fourth level administrative division, varchar(20)
// population        : bigint (8 byte int)
// elevation         : in meters, integer
// dem               : digital elevation model, srtm3 or gtopo30, average elevation of 3''x3'' (ca 90mx90m) or 30''x30'' (ca 900mx900m) area in meters, integer. srtm processed by cgiar/ciat.
// timezone          : the iana timezone id (see file timeZone.txt) varchar(40)
// modification date : date of last modification in yyyy-MM-dd format
func (p CityParser) insertCity(recordChannel chan []string, wg *sync.WaitGroup) {

	log.Debug("Attempt to insert city record")

	keyManager := keymanager.New()
	containerID := "iCloud.com.elbedev.bishcommunity"
	config := requests.RequestConfig{Version: "1", ContainerID: containerID, Database: "public"}
	requestManager := requests.New(config, &keyManager)

	template, err := p.template()

	if err != nil {
		log.WithError(err).Fatal("Failed to create template.")
	}

	i := 0
	for record := range recordChannel {
		go p.processCityRecord(record, template, &requestManager, wg, i)
		i = i + 1
	}
}

func (p CityParser) processCityRecord(record []string, template *template.Template, requestManager requests.RequestManager, wg *sync.WaitGroup, number int) {
	client := &http.Client{}
	subpath := "records/modify"

	population, _ := strconv.Atoi(record[14])
	elevation, _ := strconv.Atoi(record[15])
	latitude, _ := strconv.ParseFloat(record[4], 64)
	longitude, _ := strconv.ParseFloat(record[5], 64)

	city := model.City{
		GeoNameID:        record[0],
		Name:             record[1],
		AlternativeNames: record[3],
		Latitude:         latitude,
		Longitude:        longitude,
		CountryCode:      record[8],
		Population:       int64(population),
		Elevation:        int64(elevation),
		Timezone:         record[17]}

	var json bytes.Buffer
	err := template.Execute(&json, city)
	if err != nil {
		panic(err)
	}
	request, err := requestManager.PostRequest(subpath, json.String())
	if err != nil {
		log.WithError(err).WithField("Geoname id", record[0]).Fatal("Failed to create request for city.")
	}

	request.Header.Set("Connection", "close")
	request.Close = true
	resp, err := client.Do(request)
	if err != nil {
		log.WithError(err).WithFields(log.Fields{
			"payload": json.String(),
			"number":  number}).Error("Failed to execute request")
	}
	defer resp.Body.Close()

	log.WithFields(log.Fields{
		"Number": number,
		"Status": resp.Status,
		"City":   city.GeoNameID}).Infof("Completed %s", record[1])
	wg.Done()
}

func (p CityParser) template() (*template.Template, error) {
	return template.New("test").Parse(`{
					"operations": [
							{
									"operationType": "create",
									"record": {
											"recordType": "City",
											"fields": {
													"geonameid": {
														"value": "{{.GeoNameID}}"
													},
													"name": {
														"value": "{{.Name}}"
													},
													"alternatenames": {
															"value": "{{.AlternativeNames}}"
													},
													"location": {
														"value": {
															"latitude": {{.Latitude}},
															"longitude": {{.Longitude}}
														}
													},
													"countrycode": {
														"value": "{{.CountryCode}}"
													},
													"population": {
														"value": {{.Population}}
													},
													"elevation": {
														"value": {{.Elevation}}
													},
													"timezone": {
														"value": "{{.Timezone}}"
													}
											}
									}
							}
					]
			}`)
}
