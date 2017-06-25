package model

// City represents a city
type City struct {
	GeoNameID      string
	Name           string
	ASCIIName      string
	AlternateNames string
	Latitude       float64
	Longitude      float64
	FeatureClass   string
	FeatureCode    string
	CountryCode    string
	CC2            string
	AdminCode1     string
	AdminCode2     string
	AdminCode3     string
	AdminCode4     string
	Population     int64
	Elevation      int64
	DEM            string
	Timezone       string
}
