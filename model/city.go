package model

// City represents a city
type City struct {
	GeoNameID        string
	Name             string
	AlternativeNames string
	Latitude         float64
	Longitude        float64
	CountryCode      string
	Population       int64
	Elevation        int64
	Timezone         string
}
