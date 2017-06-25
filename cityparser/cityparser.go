package cityparser

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"html/template"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/q231950/sputnik/keymanager"
	"github.com/q231950/sputnik/requesthandling"
)

// CityParser parses cities out of a csv file
type CityParser struct {
}

// Parse parses the file given input
func (p CityParser) Parse(reader io.Reader) {
	r := csv.NewReader(reader)

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
		}

		wg.Add(1)
		recordChannel <- record
	}

	wg.Wait()

	close(recordChannel)
}

func (p CityParser) insertCity(recordChannel chan []string, wg *sync.WaitGroup) {

	for record := range recordChannel {
		type City struct {
			CountryCode, City, AccentCity, Region string
			Population                            int
			Latitude                              string
			Longitude                             string
		}

		populationInt, _ := strconv.Atoi(record[4])
		// latitudeFloat, _ := strconv.ParseFloat(record[5], 64)
		// longitudeFloat, _ := strconv.ParseFloat(record[6], 64)
		city := City{
			record[0],
			record[1],
			record[2],
			record[3],
			populationInt,
			record[5],
			record[6]}
		log.WithFields(log.Fields{
			"Country code": city.CountryCode,
			"City":         city.City,
			"Accent city":  city.AccentCity,
			"Region":       city.Region,
			"Population":   city.Population,
			"Latitude":     city.Latitude,
			"Longitude":    city.Longitude}).Info("record")

		keyManager := keymanager.New()
		containerID := "iCloud.com.elbedev.bish"
		config := requesthandling.RequestConfig{Version: "1", ContainerID: containerID}
		subpath := "records/modify"
		database := "public"
		requestManager := requesthandling.New(config, &keyManager, database)
		tmpl, err := template.New("test").Parse(`{
		      "operations": [
		          {
		              "operationType": "create",
		              "record": {
		                  "recordType": "city",
		                  "fields": {
                          "country_code": {
                            "value": "{{.CountryCode}}"
                          },
                          "accent_city":{
                            "value": "{{.AccentCity}}"
                          },
		                      "name": {
		                          "value": "{{.City}}"
		                      },
                          "region": {
                            "value": "{{.Region}}"
                          },
                          "location": {
                            "value": {
                              "latitude": {{.Latitude}},
                              "longitude": {{.Longitude}}
                            }
                          },
                          "population": {
                            "value": {{.Population}}
                          }
		                  }
		              }
		          }
		      ]
		  }`)
		if err != nil {
			panic(err)
		}

		var tpl bytes.Buffer
		err = tmpl.Execute(&tpl, city)
		if err != nil {
			panic(err)
		}
		request, err := requestManager.PostRequest(subpath, tpl.String())
		if err == nil {
			fmt.Println(request)
		} else {
			log.Fatal("Failed to create ping request")
		}

		client := &http.Client{}
		resp, err := client.Do(request)
		if err != nil {
			panic(err)
		}
		defer resp.Body.Close()

		fmt.Println("response Status:", resp.Status)
		fmt.Println("response Headers:", resp.Header)
		responseBody, _ := ioutil.ReadAll(resp.Body)
		fmt.Println("response Body:", string(responseBody))
		wg.Done()
	}
}
