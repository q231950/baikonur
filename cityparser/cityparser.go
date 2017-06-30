package cityparser

import (
	"bytes"
	"encoding/csv"
	"html/template"
	"io"
	"net/http"
	"strconv"
	"sync"

	log "github.com/Sirupsen/logrus"
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
	r.Comma = ';'

	recordChannel := make(chan []string, 10)
	wg := new(sync.WaitGroup)

	go p.insertCity(recordChannel, wg)

	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		} else {
			wg.Add(1)
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

	log.Warn("Inserting a city")

	keyManager := keymanager.New()
	containerID := "iCloud.com.elbedev.bish"
	config := requests.NewRequestConfig("1", containerID)
	subpath := "records/modify"
	database := "public"
	requestManager := requests.New(config, &keyManager, database)

	tmpl, err := p.template()

	if err != nil {
		panic(err)
	}

	client := &http.Client{}

	for record := range recordChannel {

		population, _ := strconv.Atoi(record[14])
		elevation, _ := strconv.Atoi(record[15])
		latitude, _ := strconv.ParseFloat(record[4], 64)
		longitude, _ := strconv.ParseFloat(record[5], 64)

		city := model.City{
			GeoNameID:      record[0],
			Name:           record[1],
			ASCIIName:      record[2],
			AlternateNames: record[3],
			Latitude:       latitude,
			Longitude:      longitude,
			FeatureClass:   record[6],
			FeatureCode:    record[7],
			CountryCode:    record[8],
			CC2:            record[9],
			AdminCode1:     record[10],
			AdminCode2:     record[11],
			AdminCode3:     record[12],
			AdminCode4:     record[13],
			Population:     int64(population),
			Elevation:      int64(elevation),
			DEM:            record[16],
			Timezone:       record[17]}

		var tpl bytes.Buffer
		err = tmpl.Execute(&tpl, city)
		if err != nil {
			panic(err)
		}
		request, err := requestManager.PostRequest(subpath, tpl.String())
		if err != nil {
			log.Fatal("Failed to create request")
		}

		resp, err := client.Do(request)
		if err != nil {
			log.Error("Failed to execute request", request)
			panic(err)
		}

		log.WithFields(log.Fields{"Status": resp.Status, "City": city.GeoNameID}).Info("")
		wg.Done()
	}
}

func (p CityParser) template() (*template.Template, error) {
	return template.New("test").Parse(`{
					"operations": [
							{
									"operationType": "create",
									"record": {
											"recordType": "cities",
											"fields": {
													"geonameid": {
														"value": "{{.GeoNameID}}"
													},
													"name": {
														"value": "{{.Name}}"
													},
													"asciiname": {
															"value": "{{.ASCIIName}}"
													},
													"alternatenames": {
														"value": "{{.AlternateNames}}"
													},
													"location": {
														"value": {
															"latitude": {{.Latitude}},
															"longitude": {{.Longitude}}
														}
													},
													"feature_class": {
														"value": "{{.FeatureClass}}"
													},
													"feature_code": {
														"value": "{{.FeatureCode}}"
													},
													"country_code": {
														"value": "{{.CountryCode}}"
													},
													"cc2": {
														"value": "{{.CC2}}"
													},
													"admin1_code": {
														"value": "{{.AdminCode1}}"
													},
													"admin2_code": {
														"value": "{{.AdminCode2}}"
													},
													"admin3_code": {
														"value": "{{.AdminCode3}}"
													},
													"admin4_code": {
														"value": "{{.AdminCode4}}"
													},
													"population": {
														"value": {{.Population}}
													},
													"elevation": {
														"value": {{.Elevation}}
													},
													"dem": {
														"value": "{{.DEM}}"
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
