package cityparser

import (
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"

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
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		p.insertCity(record)
	}
}

func (p CityParser) insertCity(record []string) {
	log.WithFields(log.Fields{
		"Country code": record[0],
		"City":         record[1],
		"Accent city":  record[2],
		"Region":       record[3],
		"Population":   record[4],
		"Latitude":     record[5],
		"Longitude":    record[6]}).Info("record")

	keyManager := keymanager.New()
	containerID := "iCloud.com.elbedev.bish"
	config := requesthandling.RequestConfig{Version: "1", ContainerID: containerID}
	subpath := "records/modify"
	database := "public"
	requestManager := requesthandling.New(config, &keyManager, database)
	body := `{
      "operations": [
          {
              "operationType": "create",
              "record": {
                  "recordType": "city",
                  "fields": {
                      "name": {
                          "value": "bishkek"
                      }
                  }
              }
          }
      ]
  }`
	request, err := requestManager.PostRequest(subpath, body)
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
}
