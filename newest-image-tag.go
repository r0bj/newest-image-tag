// Compatible with container registries supporting Image Manifest Version 2, Schema 1
// https://docs.docker.com/registry/spec/manifest-v2-1/

package main

import (
	"os"
	"fmt"
	"strings"
	"encoding/json"
	"time"
	"sort"
	"io/ioutil"

	"gopkg.in/alecthomas/kingpin.v2"
	log "github.com/sirupsen/logrus"
	"github.com/parnurzeal/gorequest"
	"github.com/go-redis/redis/v7"
)

const (
	ver string = "0.1"
	logDateLayout string = "2006-01-02 15:04:05"
	httpTimeout int = 10
)

var (
	redisPassword = kingpin.Flag("redis-password", "Redis password.").Default("").String()
	redisKeyTTL = kingpin.Flag("redis-key-ttl", "Redis key TTL in seconds.").Default("604800").Int()
	redisDB = kingpin.Flag("redis-db", "Redis database.").Default("0").Short('d').Int()
	redisHost = kingpin.Flag("redis-host", "Redis host address.").Default("localhost").Short('r').String()
	redisPort = kingpin.Flag("redis-port", "Redis port.").Default("6379").String()
	username = kingpin.Flag("username", "Username for container registry.").Short('u').String()
	password = kingpin.Flag("password", "Password for container registry.").Short('p').String()
	passwordFile = kingpin.Flag("password-file", "Path to file with password for container registry.").String()
	verbose = kingpin.Flag("verbose", "Verbose mode.").Short('v').Bool()
	tagWithImage = kingpin.Flag("full-image", "Return tag with image name.").Short('f').Default("true").Bool()
	cache = kingpin.Flag("cache", "Don't use redis as a cache.").Default("true").Bool()
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
	Host string
	Path string
}

// HTTPResponse : containts HTTP response data
type HTTPResponse struct {
	body string
	err error
}

// ImageTag : containts image tags create time
type ImageTag struct {
	Tag string
	Date time.Time
}

func parseImageName(imageName string) (ImageParts, error) {
	var imageParts ImageParts 

	parts := strings.Split(imageName, "/")
	if len(parts) < 2 {
		return imageParts, fmt.Errorf("Image name parse error, not all required parts detected")
	}

	imageParts.Host = parts[0]
    imageParts.Path = strings.Join(parts[1:], "/")

	return imageParts, nil
}

func httpGet(url, basicAuthUser, basicAuthPassword string, response chan<- HTTPResponse) {
	var msg HTTPResponse

	request := gorequest.New()

	if basicAuthUser != "" && basicAuthPassword != "" {
		request = gorequest.New().SetBasicAuth(basicAuthUser, basicAuthPassword)
	}

	resp, body, errs := request.Get(url).Set("Accept", "application/vnd.docker.distribution.manifest.v1+prettyjws").End()

	if errs != nil {
		var errsStr []string
		for _, e := range errs {
			errsStr = append(errsStr, fmt.Sprintf("%s", e))
		}
		msg.err = fmt.Errorf("%s", strings.Join(errsStr, ", "))
		response <- msg
		return
	}

	if resp.StatusCode == 200 {
		msg.body = body
	} else {
		msg.err = fmt.Errorf("HTTP response code: %s", resp.Status)
	}
	response <- msg
}

func getTagsList(image, username, password string) (TagList, error) {
	var tagList TagList
	log.Debugf("Getting container registry tags list for %s", image)

	imageParts, err := parseImageName(image)
	if err != nil {
		return tagList, err
	}

	response := make(chan HTTPResponse)
	url := "https://" + imageParts.Host + "/v2/" + imageParts.Path + "/tags/list"
	go httpGet(url, username, password, response)

	select {
	case msg := <-response:
		if msg.err == nil {
			err := json.Unmarshal([]byte(msg.body), &tagList)
			if err != nil {
				return tagList, fmt.Errorf("unmarshall error: %v", err)
			}
		} else {
			return tagList, msg.err
		}
	case <-time.After(time.Second * time.Duration(httpTimeout)):
		return tagList, fmt.Errorf("%s: container registry http timeout", url)
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

	response := make(chan HTTPResponse)
	url := "https://" + imageParts.Host + "/v2/" + imageParts.Path + "/manifests/" + tag
	go httpGet(url, username, password, response)

	select {
	case msg := <-response:
		if msg.err == nil {
			err := json.Unmarshal([]byte(msg.body), &tagManifest)
			if err != nil {
				return tagManifest, fmt.Errorf("unmarshall error: %v", err)
			}
		} else {
			return tagManifest, msg.err
		}
	case <-time.After(time.Second * time.Duration(httpTimeout)):
		return tagManifest, fmt.Errorf("%s: container registry http timeout", url)
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

	date, err := getNewestManifestHistoryItem(manifest)
	if err != nil {
		return time.Time{}, err
	}

	return date, nil
}

func getTagDateUsingCache(image, tagName, username, password string, redisClient *redis.Client) (ImageTag, error) {
	var imageTag ImageTag
	imageTag.Tag = tagName

	imageWithTag := image + ":" + tagName
		
	if *cache {
		dateStr, err := redisClient.Get(imageWithTag).Result()
		if err == redis.Nil {
			log.Debugf("Image tag %s not in cache, calling container registry", imageWithTag)

			imageTag.Date, err = getTagDate(image, tagName, username, password)
			if err != nil {
				return imageTag, err
			}

			err = redisClient.Set(imageWithTag, imageTag.Date.Format(time.RFC3339Nano), time.Duration(*redisKeyTTL) * time.Second).Err()
			if err != nil {
				return imageTag, err
			}

		} else if err != nil {
			return imageTag, err
		} else {
			log.Debugf("Image tag %s present in cache", imageWithTag)

			date, err := time.Parse(time.RFC3339Nano, dateStr)
			if err != nil {
				return imageTag, err
			}

			imageTag.Date = date
		}
	} else {
		var err error
		imageTag.Date, err = getTagDate(image, tagName, username, password)
		if err != nil {
			return imageTag, err
		}		
	}

	return imageTag, nil
}

func getNewestTag(image, username, password string, redisClient *redis.Client) (string, error) {
	tagList, err := getTagsList(image, username, password)
	if err != nil {
		return "", err
	}

	var tags []ImageTag
	for _, tagName := range tagList.Tags {
		tag, err := getTagDateUsingCache(image, tagName, username, password, redisClient)
		if err != nil {
			return "", err
		}

		tags = append(tags, tag)
	}

	sort.Slice(tags, func(i, j int) bool {
	    return tags[i].Date.After(tags[j].Date)
	})

	return tags[0].Tag, nil
}

func main() {
	customFormatter := new(log.TextFormatter)
	customFormatter.TimestampFormat = logDateLayout
	log.SetFormatter(customFormatter)
	customFormatter.FullTimestamp = true
	log.SetOutput(os.Stdout)

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

	tag, err := getNewestTag(*image, *username, registryPassword, client)
	if err != nil {
		log.Fatal(err)
	}

	if *tagWithImage {
		fmt.Println(*image + ":" + tag)
	} else {
		fmt.Println(tag)
	}
}
