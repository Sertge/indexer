package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/mail"
	"os"
	"path/filepath"
	"strings"
)

func main() {
	err := createIndex()
	if err != nil {
		log.Fatal(err)
	}

	err = filepath.Walk("../enron_mail_20110402/maildir", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}

		// Read file content
		content, err := ioutil.ReadFile(path)
		if err != nil {
			return err
		}

		// Create document ID
		// fmt.Println(path)
		paths := strings.Split(path, "\\")
		docID := strings.Replace(path, "/", "_", -1)
		docID = strings.Replace(docID, ".", "_", -1)
		if len(paths) == 6 {
			postOnIndex(paths, content)
		}

		return nil
	})

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Indexing complete")
}

func createIndex() error {
	// This creates the index if it doesn't exist
	client := &http.Client{}
	headUrl := "http://localhost:4080/api/index/mailsIndex"
	headr, err := http.NewRequest("HEAD", headUrl, nil)
	if err != nil {
		log.Fatal(err)
	}
	headr.SetBasicAuth("admin", "Complexpass#123")
	res, err := client.Do(headr)
	if err != nil {
		log.Fatal(err)
	}

	if res.StatusCode == 404 {
		postUrl := "http://localhost:4080/api/index"
		newIndex := []byte(`{
			"name": "mailsIndex",
			"mappings": {
				"properties": {
					"username": {
						"type": "text",
						"index": true,
						"store": true,
						"highlightable": true
					},
					"content": {
						"type": "text",
						"index": true,
						"store": true,
						"highlightable": true
					},
					"date": {
						"type": "keyword",
						"format": "2006-01-02T15:04:05Z-05:00",
						"index": true,
						"sortable": true,
						"aggregatable": true
					},
					"category": {
						"type": "text",
						"index": true,
						"store": true,
						"highlightable": true
					}
				}
			} 
		}`)

		r, err := http.NewRequest("POST", postUrl, bytes.NewBuffer(newIndex))
		if err != nil {
			log.Fatal(err)
		}

		r.SetBasicAuth("admin", "Complexpass#123")
		_, err = client.Do(r)

		if err != nil {
			log.Fatal(err)
		}
	}

	return nil
}

func postOnIndex(paths []string, content []byte) error {
	contentAsString := string(content)
	contentAsString = strings.ReplaceAll(contentAsString, "\"", "\\\"")

	contentAsMail, err := mail.ReadMessage(strings.NewReader(contentAsString))

	if err != nil {
		log.Fatal(err)
	}
	mailBody, err := io.ReadAll(contentAsMail.Body)
	mailDate, err := mail.ParseDate(contentAsMail.Header.Get("Date"))
	postUrl := "http://localhost:4080/api/mailsIndex/_doc"
	postBuffer := []byte(fmt.Sprintf(`{
		"username": "%v",
		"date": "%v",
		"category": "%v",
		"content": "%s"
	}`, paths[3], mailDate, paths[4], mailBody))
	r, err := http.NewRequest("POST", postUrl, bytes.NewBuffer(postBuffer))
	r.SetBasicAuth("admin", "Complexpass#123")
	client := &http.Client{}
	res, err := client.Do(r)
	if err != nil {
		log.Fatal(err)
	}
	if res.StatusCode != 200 {
		fmt.Println(paths)
	}
	defer res.Body.Close()
	return nil
}
