// Compatible with container registries supporting Image Manifest Version 2, Schema 1
// https://docs.docker.com/registry/spec/manifest-v2-1/

package main

import (
	"fmt"
	"strings"
	"encoding/json"
	"time"
	"sort"
	"io/ioutil"
	"net/http"

	"gopkg.in/alecthomas/kingpin.v2"
	log "github.com/sirupsen/logrus"
	"github.com/go-redis/redis/v7"
)

const (
	ver string = "0.10"
	logDateLayout string = "2006-01-02 15:04:05"
	httpTimeout int = 10
	retries int = 3
	sleepInterval int = 3
)

var (
	redisPassword = kingpin.Flag("redis-password", "Redis password.").Default("").String()
	redisKeyTTL = kingpin.Flag("redis-key-ttl", "Redis key TTL in seconds.").Default("604800").Int()
	redisDB = kingpin.Flag("redis-db", "Redis database.").Default("0").Short('d').Int()
	redisHost = kingpin.Flag("redis-host", "Redis host address.").Default("localhost").Short('r').String()
	redisPort = kingpin.Flag("redis-port", "Redis port.").Default("6379").String()
	username = kingpin.Flag("username", "Username for container registry.").Default("anonymous").Short('u').String()
	password = kingpin.Flag("password", "Password for container registry.").Default("anonymous").Short('p').String()
	passwordFile = kingpin.Flag("password-file", "Path to file with password for container registry.").String()
	verbose = kingpin.Flag("verbose", "Verbose mode.").Short('v').Bool()
	cache = kingpin.Flag("cache", "Use redis as a cache.").Bool()
	threads = kingpin.Flag("threads", "Number of threads for accessing registry.").Default("30").Int()
	jsonOutput = kingpin.Flag("json-output", "Generate output in JSON format.").Short('j').Bool()
	image = kingpin.Arg("image", "Image name.").Required().String()
)

// TagList : containts image tag list data
type TagList struct {
	Name string `json:"name"`
	Tags []string `json:"tags"`
}

// TagManifest : containts image tag manifest data
type TagManifest struct {
	Name string `json:"name"`
	SchemaVersion int `json:"schemaVersion"`
	History []struct {
		V1Compatibility string `json:"v1Compatibility"`
	} `json:"history"`
}

// ManifestHistoryItem : containts manifest history data
type ManifestHistoryItem struct {
	Created string `json:"created"`
}

// ImageParts : containts image parts
type ImageParts struct {
	host string
	path string
}

// HTTPResponse : containts HTTP response data
type HTTPResponse struct {
	body string
	err error
	statusCode int
}

// ImageTag : containts image tags create time
type ImageTag struct {
	tag string
	date time.Time
	err error
}

// Output : containts stdout output
type Output struct {
	Tag string `json:"tag"`
	Image string `json:"image"`
	ImageWithTag string `json:"imageWithTag"`
}

func parseImageName(imageName string) (ImageParts, error) {
	var imageParts ImageParts 

	parts := strings.Split(imageName, "/")
	if len(parts) < 2 {
		return imageParts, fmt.Errorf("Image name parse error, not all required parts detected")
	}

	imageParts.host = parts[0]
    imageParts.path = strings.Join(parts[1:], "/")

	return imageParts, nil
}

func httpGet(url, basicAuthUser, basicAuthPassword string, response chan<- HTTPResponse) {
	var msg HTTPResponse

	client := &http.Client{}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		msg.err = err
		return
	}

	if basicAuthUser != "" && basicAuthPassword != "" {
		req.SetBasicAuth(basicAuthUser, basicAuthPassword)
	}

	resp, err := client.Do(req)
	if err != nil {
		msg.err = err
		return
	}

	msg.statusCode = resp.StatusCode

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		msg.err = err
		return
	}

	msg.body = string(body)
	response <- msg

	return
}

func retryGetRequest(url, username, password string) (string, error) {
	var responseError error
	var body string

	response := make(chan HTTPResponse)
	Loop:
		for i := 1; i <= retries; i++ {
			if i != 1 {
				log.Debugf("Retrying request %s", url)
				time.Sleep(time.Second * time.Duration(sleepInterval))
			}
			go httpGet(url, username, password, response)

			select {
			case msg := <-response:
				if msg.err == nil {
					if msg.statusCode == 200 {
						body = msg.body
						break Loop
					} else if msg.statusCode >= 500 {
						responseError = fmt.Errorf("%s: HTTP response code: %d", url, msg.statusCode)
						continue Loop
					} else {
						responseError = fmt.Errorf("%s: HTTP response code: %d", url, msg.statusCode)
						break Loop
					}
				} else {
					responseError = msg.err
					break Loop
				}
			case <-time.After(time.Second * time.Duration(httpTimeout)):
				responseError = fmt.Errorf("%s: HTTP response timeout", url)
				continue Loop
			}
		}

	if responseError != nil {
		return "", responseError
	}

	return body, nil
}

func getTagsList(image, username, password string) (TagList, error) {
	var tagList TagList
	log.Debugf("Getting container registry tags list for %s", image)

	imageParts, err := parseImageName(image)
	if err != nil {
		return tagList, err
	}

	url := "https://" + imageParts.host + "/v2/" + imageParts.path + "/tags/list"
	body, err := retryGetRequest(url, username, password)
	if err != nil {
		return tagList, err
	}

	if err := json.Unmarshal([]byte(body), &tagList); err != nil {
		return tagList, fmt.Errorf("Unmarshal body failed: %v", err)
	}

	return tagList, nil
}

func getTagManifest(image, tag, username, password string) (TagManifest, error) {
	var tagManifest TagManifest
	log.Debugf("Getting container registry manifest for tag %s", tag)

	imageParts, err := parseImageName(image)
	if err != nil {
		return tagManifest, err
	}

	url := "https://" + imageParts.host + "/v2/" + imageParts.path + "/manifests/" + tag
	body, err := retryGetRequest(url, username, password)
	if err != nil {
		return tagManifest, err
	}

	if err := json.Unmarshal([]byte(body), &tagManifest); err != nil {
		return tagManifest, fmt.Errorf("Unmarshal body failed: %v", err)
	}

	return tagManifest, nil
}

func getNewestManifestHistoryItem(tagManifest TagManifest) (time.Time, error) {
	var createDates []time.Time

	for _, item := range tagManifest.History {
		var manifestHistoryItem ManifestHistoryItem

		err := json.Unmarshal([]byte(item.V1Compatibility), &manifestHistoryItem)
		if err != nil {
			return time.Time{}, err
		}

		date, err := time.Parse(time.RFC3339Nano, manifestHistoryItem.Created)
		if err != nil {
			return time.Time{}, err
		}
		createDates = append(createDates, date)
	}

	sort.Slice(createDates, func(i, j int) bool {
	    return createDates[i].After(createDates[j])
	})

	return createDates[0], nil
}

func getTagDate(image, tagName, username, password string) (time.Time, error) {
	manifest, err := getTagManifest(image, tagName, username, password)
	if err != nil {
		return time.Time{}, err
	}

	if manifest.SchemaVersion != 1 {
		return time.Time{}, fmt.Errorf("Wrong image manifest version, should be Image Manifest Version 2, Schema 1: https://docs.docker.com/registry/spec/manifest-v2-1")
	}

	date, err := getNewestManifestHistoryItem(manifest)
	if err != nil {
		return time.Time{}, err
	}

	return date, nil
}

func getTagDateUsingCache(image, username, password string, redisClient *redis.Client, tags <-chan string, results chan<- ImageTag) {
	for tagName := range tags {
		var imageTag ImageTag
		imageTag.tag = tagName

		imageWithTag := image + ":" + tagName

		if *cache {
			dateStr, err := redisClient.Get(imageWithTag).Result()
			if err == redis.Nil {
				log.Debugf("Image tag %s not in cache, calling container registry", imageWithTag)

				imageTag.date, err = getTagDate(image, tagName, username, password)
				if err != nil {
					imageTag.err = err
					results <- imageTag
					break
				}

				err = redisClient.Set(imageWithTag, imageTag.date.Format(time.RFC3339Nano), time.Duration(*redisKeyTTL) * time.Second).Err()
				if err != nil {
					imageTag.err = err
					results <- imageTag
					break
				}

			} else if err != nil {
				imageTag.err = fmt.Errorf("Cannot connect to redis: %v", err)
				results <- imageTag
				break
			} else {
				log.Debugf("Image tag %s present in cache", imageWithTag)

				date, err := time.Parse(time.RFC3339Nano, dateStr)
				if err != nil {
					imageTag.err = err
					results <- imageTag
					break
				}

				imageTag.date = date
			}
		} else {
			var err error
			imageTag.date, err = getTagDate(image, tagName, username, password)
			if err != nil {
				imageTag.err = err
				results <- imageTag
				break
			}
		}

		results <- imageTag
	}
}

func getNewestTag(image, username, password string, redisClient *redis.Client) (Output, error) {
	var output Output
	output.Image = image

	tagList, err := getTagsList(image, username, password)
	if err != nil {
		return output, err
	}

	numJobs := len(tagList.Tags)
	jobs := make(chan string, numJobs)
	results := make(chan ImageTag, numJobs)

	for w := 1; w <= *threads; w++ {
		go getTagDateUsingCache(image, username, password, redisClient, jobs, results)
	}

	for _, tagName := range tagList.Tags {
		jobs <- tagName
	}
	close(jobs)

	var tags []ImageTag
	for a := 1; a <= numJobs; a++ {
		tag := <-results
		if tag.err != nil {
			return output, tag.err
		}
		tags = append(tags, tag)
	}

	sort.Slice(tags, func(i, j int) bool {
	    return tags[i].date.After(tags[j].date)
	})

	output.Tag = tags[0].tag
	output.ImageWithTag = image + ":" + tags[0].tag
	return output, nil
}

func main() {
	customFormatter := new(log.TextFormatter)
	customFormatter.TimestampFormat = logDateLayout
	log.SetFormatter(customFormatter)
	customFormatter.FullTimestamp = true

	kingpin.Version(ver)
	kingpin.Parse()

	if *verbose {
		log.SetLevel(log.DebugLevel)
	}

	var registryPassword string
	if *passwordFile != "" {
		file, err := ioutil.ReadFile(*passwordFile)
		if err != nil {
			log.Fatal(err)
		}
		registryPassword = string(file)
	} else {
		registryPassword = *password
	}

	client := redis.NewClient(&redis.Options{
		Addr: *redisHost + ":" + *redisPort,
		Password: *redisPassword,
		DB: *redisDB,
	})

	output, err := getNewestTag(*image, *username, registryPassword, client)
	if err != nil {
		log.Fatal(err)
	}

	if *jsonOutput {
		outputJson, _ := json.Marshal(output)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println(string(outputJson))
	} else {
		fmt.Println(output.ImageWithTag)
	}
}
